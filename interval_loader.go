package cidataloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
)

type intervalLoader struct {
	instanceTime *time.Time
	jobRunName   string
	source       string
}

type KeyValue struct {
	Key string `json:"key" bigquery:"key"`
	Val string `json:"value" bigquery:"value"`
}

type Locator struct {
	Type                  string     `json:"type" bigquery:"type"`
	Hmsg                  string     `json:"hmsg" bigquery:"hmsg"`
	Namespace             string     `json:"namespace" bigquery:"namespace"`
	Node                  string     `json:"node" bigquery:"node"`
	Pod                   string     `json:"pod" bigquery:"pod"`
	Uid                   string     `json:"uid" bigquery:"uid"`
	Container             string     `json:"container" bigquery:"container"`
	E2eTest               string     `json:"e2eTest" bigquery:"e2eTest"`
	BackendDisruptionName string     `json:"backend_disruption_name" bigquery:"backend_disruption_name"`
	Keys                  []KeyValue `json:"keys" bigquery:"keys"`
}

type Message struct {
	Reason         string     `json:"reason" bigquery:"reason"`
	Cause          string     `json:"cause" bigquery:"cause"`
	HumanMessage   string     `json:"human_message" bigquery:"human_message"`
	Container      string     `json:"container" bigquery:"container"`
	FirstTimestamp time.Time  `json:"firstimestamp" bigquery:"firstTimestamp"`
	LastTimestamp  time.Time  `json:"lastimestamp" bigquery:"lastTimestamp"`
	Image          string     `json:"image" bigquery:"image"`
	Constructed    string     `json:"constructed" bigquery:"constructed"`
	Status         string     `json:"status" bigquery:"status"`
	Node           string     `json:"node" bigquery:"node"`
	Annotations    []KeyValue `json:"annotations" bigquery:"annotations"`
}
type ComplexInterval struct {
	Locator        Locator   `json:"locator" bigquery:"locator"`
	Message        Message   `json:"message" bigquery:"message"`
	From           time.Time `json:"from_time" bigquery:"from_time"`
	To             time.Time `json:"to_time" bigquery:"to_time"`
	IntervalSource string    `json:"interval_source" bigquery:"interval_source"`
	JobRunName     string    `json:"JobRunName" bigquery:"JobRunName"`
	Source         string    `json:"source" bigquery:"source"`
}

func (i *intervalLoader) getJSONObject(client *storage.Client, ctx context.Context, bucket, name string) (map[string]any, error) {
	o := client.Bucket(bucket).Object(name)

	if i.instanceTime == nil {
		attrs, err := o.Attrs(ctx)
		if err != nil {
			return nil, err
		}

		if attrs.Created.IsZero() {
			return nil, fmt.Errorf("invalid creation time")
		}

		i.instanceTime = &attrs.Created
	}

	r, err := o.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	defer r.Close()

	b, err := io.ReadAll(r)

	if err != nil {
		return nil, err
	}

	return i.getJSON(b)
}

func (i *intervalLoader) getJSON(bytes []byte) (map[string]any, error) {
	var result map[string]any
	err := json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func generateComplexIntervalLoader(client *storage.Client, ctx context.Context, event *JobRunDataEvent, dataLoader *BigQueryLoader) error {

	i := intervalLoader{}
	if !event.GCSEvent.TimeCreated.IsZero() {
		i.instanceTime = &event.GCSEvent.TimeCreated
	}

	i.source = event.Filename
	i.jobRunName = event.BuildID
	rows, err := i.parseComplexRows(client, ctx, event.GCSEvent.Bucket, event.GCSEvent.Name)
	if err != nil {
		return err
	}

	dataFile := DataFile{
		TableName:       "e2e_intervals",
		Schema:          nil,
		SchemaMapping:   nil,
		ComplexRows:     rows,
		ExpirationDays:  0,
		PartitionColumn: "from_time",
		PartitionType:   "",
		ChunkSize:       1000,
	}
	dataInstance := DataInstance{CreationTime: *i.instanceTime, JobRunName: event.BuildID, Source: event.Filename, DataFile: &dataFile}
	existing, err := dataLoader.FindExistingData(ctx, dataInstance.CreationTime, dataFile.PartitionColumn, dataFile.TableName, dataInstance.JobRunName, dataInstance.Source)

	if err != nil {
		return err
	} else if existing {
		// we don't want duplicate data so if we have records already then log a warning and return nil so the function doesn't retry
		logwithctx(ctx).Warnf("found existing data for %s/%s", dataInstance.JobRunName, dataInstance.Source)
		return nil
	}

	_, err = dataLoader.LoadComplexDataItems(ctx, dataInstance)
	if err != nil {
		return err
	}

	return nil

}

func generateIntervalUploader(client *storage.Client, ctx context.Context, event *JobRunDataEvent, dataLoader DataLoader) (SimpleUploader, error) {
	i := intervalLoader{}
	if !event.GCSEvent.TimeCreated.IsZero() {
		i.instanceTime = &event.GCSEvent.TimeCreated
	}

	rows, err := i.parseRows(client, ctx, event.GCSEvent.Bucket, event.GCSEvent.Name)
	if err != nil {
		return nil, err
	}

	dataInstance := DataInstance{CreationTime: *i.instanceTime, JobRunName: event.BuildID, Source: event.Filename}
	dataFile := DataFile{
		TableName:       "e2e_intervals",
		Schema:          map[string]DataType{"from_time": DataTypeTimestamp, "to_time": DataTypeTimestamp, "interval_json": DataTypeJSON},
		SchemaMapping:   map[string]string{"from": "from_time", "to": "to_time", "interval": "interval_json"},
		PartitionColumn: "from_time",
		ChunkSize:       5000,
		Rows:            rows,
	}

	dataInstance.DataFile = &dataFile

	loader, err := generateDataUploader(dataInstance, dataLoader)
	if err != nil {
		return nil, err
	}

	return loader, nil
}

func (i *intervalLoader) parseComplexRows(client *storage.Client, ctx context.Context, bucket, name string) ([]interface{}, error) {
	intervals, err := i.getJSONObject(client, ctx, bucket, name)

	if err != nil {
		return nil, err
	}

	rows := make([]interface{}, 0)

	if items, ok := intervals["items"]; ok {
		k := reflect.ValueOf(items).Kind()
		if k != reflect.Slice {
			return nil, fmt.Errorf("top interval items are not an array")
		}

		for _, it := range items.([]any) {

			if reflect.ValueOf(it).Kind() == reflect.Map {

				item := it.(map[string]interface{})

				// want from, to and then the whole map as a json string

				var fromTime, toTime time.Time
				if f, ok := item["from"]; ok {
					from := f.(string)
					fromt, err := time.Parse(time.RFC3339, from)
					if err != nil {
						logrus.Errorf("Failed to parse from field for interval: %v", item)
						continue
					}
					fromTime = fromt
				} else {
					logrus.Errorf("Invalid from field for interval: %v", item)
					continue
				}

				if t, ok := item["to"]; ok && t != nil {
					to := t.(string)
					tot, err := time.Parse(time.RFC3339, to)
					if err == nil {
						toTime = tot
					}
				}

				source := ""
				if s, ok := item["source"]; ok && s != nil {
					source = s.(string)
				}

				//b, err := json.Marshal(item)
				//if err != nil {
				//	logrus.Errorf("Failed to marshal interval: %v", item)
				//	continue
				//}

				locator := Locator{}
				if l, ok := item["locator"]; ok && l != nil {
					if reflect.ValueOf(l).Kind() == reflect.Map {
						locatorValues := l.(map[string]interface{})
						// logrus.Infof("Locator: %v", locatorValues)

						if t, ok := locatorValues["type"]; ok && t != nil {
							locator.Type = t.(string)
						}

						if a, ok := locatorValues["keys"]; ok && a != nil {
							if reflect.ValueOf(a).Kind() == reflect.Map {
								keys := a.(map[string]interface{})
								for k, v := range keys {
									value := fmt.Sprintf("%v", v)
									switch {
									case strings.EqualFold(k, "hmsg"):
										locator.Hmsg = value

									case strings.EqualFold(k, "namespace"):
										locator.Namespace = value

									case strings.EqualFold(k, "node"):
										locator.Node = value

									case strings.EqualFold(k, "pod"):
										locator.Pod = value

									case strings.EqualFold(k, "uid"):
										locator.Uid = value

									case strings.EqualFold(k, "container"):
										locator.Container = value

									case strings.EqualFold(k, "e2e-test"):
										locator.E2eTest = value

									case strings.EqualFold(k, "backend-disruption-name"):
										locator.BackendDisruptionName = value

									default:
										kv := KeyValue{}
										kv.Key = k
										kv.Val = value
										locator.Keys = append(locator.Keys, kv)
									}
								}
							}
						}

					}
				}

				message := Message{}
				if m, ok := item["message"]; ok && m != nil {
					if reflect.ValueOf(m).Kind() == reflect.Map {
						messageValues := m.(map[string]interface{})
						// logrus.Infof("Message: %v", messageValues)

						if a, ok := messageValues["annotations"]; ok && a != nil {
							if reflect.ValueOf(a).Kind() == reflect.Map {
								annotations := a.(map[string]interface{})
								for k, v := range annotations {
									value := fmt.Sprintf("%v", v)
									switch {
									case strings.EqualFold(k, "cause"):
										message.Cause = value

									case strings.EqualFold(k, "human_message"):
										message.HumanMessage = value

									case strings.EqualFold(k, "container"):
										message.Container = value

									case strings.EqualFold(k, "image"):
										message.Image = value

									case strings.EqualFold(k, "constructed"):
										message.Constructed = value

									case strings.EqualFold(k, "status"):
										message.Status = value

									case strings.EqualFold(k, "node"):
										message.Node = value

									case strings.EqualFold(k, "firstTimestamp"):
										valueTime, err := time.Parse(time.RFC3339, value)
										if err == nil {
											message.FirstTimestamp = valueTime
										}

									case strings.EqualFold(k, "lastTimestamp"):
										valueTime, err := time.Parse(time.RFC3339, value)
										if err == nil {
											message.LastTimestamp = valueTime
										}

									default:
										kv := KeyValue{}
										kv.Key = k
										kv.Val = value
										message.Annotations = append(message.Annotations, kv)
									}

								}
							}
						}
					}
				}

				row := ComplexInterval{
					Locator:        locator,
					Message:        message,
					From:           fromTime,
					To:             toTime,
					IntervalSource: source,
					JobRunName:     i.jobRunName,
					Source:         i.source,
				}
				rows = append(rows, &row)
			} else {
				logrus.Errorf("Invalid item type: %v", reflect.ValueOf(it))
			}
		}

	} else {
		return nil, fmt.Errorf("failed to parse top interval items")
	}

	return rows, nil
}

func (i *intervalLoader) parseRows(client *storage.Client, ctx context.Context, bucket, name string) ([]map[string]string, error) {

	intervals, err := i.getJSONObject(client, ctx, bucket, name)

	if err != nil {
		return nil, err
	}

	rows := make([]map[string]string, 0)

	if items, ok := intervals["items"]; ok {
		k := reflect.ValueOf(items).Kind()
		if k != reflect.Slice {
			return nil, fmt.Errorf("top interval items are not an array")
		}

		for _, it := range items.([]any) {

			if reflect.ValueOf(it).Kind() == reflect.Map {

				item := it.(map[string]interface{})

				// want from, to and then the whole map as a json string
				var from, to, jval string
				if f, ok := item["from"]; ok {
					from = f.(string)
				} else {
					logrus.Errorf("Invalid from field for interval: %v", item)
					continue
				}

				if t, ok := item["to"]; ok && t != nil {
					to = t.(string)
				}
				// else {
				// 	logrus.Errorf("Invalid to field for interval: %v", item)
				// 	continue
				// }

				b, err := json.Marshal(item)
				if err != nil {
					logrus.Errorf("Failed to marshal interval: %v", item)
					continue
				}

				jval = string(b)
				row := map[string]string{"from": from, "to": to, "interval": jval}
				rows = append(rows, row)
			} else {
				logrus.Errorf("Invalid item type: %v", reflect.ValueOf(it))
			}
		}

	} else {
		return nil, fmt.Errorf("failed to parse top interval items")
	}

	return rows, nil

}
