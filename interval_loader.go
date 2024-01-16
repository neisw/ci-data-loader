package cidataloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
)

type intervalLoader struct {
	instanceTime *time.Time
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
		Schema:          map[string]DataType{"from_time": DataTypeTimestamp, "to_time": DataTypeTimestamp, "interval": DataTypeJSON},
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
