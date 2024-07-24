package cidataloader

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"
)

type metricsLoader struct {
	instanceTime *time.Time
}

func generateMetricsUploader(client *storage.Client, ctx context.Context, event *JobRunDataEvent, dataLoader DataLoader) (SimpleUploader, error) {

	m := metricsLoader{}
	if !event.GCSEvent.TimeCreated.IsZero() {
		m.instanceTime = &event.GCSEvent.TimeCreated
	}

	rows, err := m.parseRows(client, ctx, event.GCSEvent.Bucket, event.GCSEvent.Name)
	if err != nil {
		return nil, err
	}

	dataInstance := DataInstance{CreationTime: *m.instanceTime, JobRunName: event.BuildID, Source: event.Filename}
	dataFile := DataFile{
		TableName: "e2e_metrics",
		Schema:    map[string]DataType{"value": DataTypeFloat64, "timestamp": DataTypeInteger, "key": DataTypeString},
		ChunkSize: 1000,
		Rows:      rows,
	}

	dataInstance.DataFile = &dataFile

	loader, err := generateDataUploader(dataInstance, dataLoader)
	if err != nil {
		return nil, err
	}

	return loader, nil

}

func (m *metricsLoader) parseRows(client *storage.Client, ctx context.Context, bucket, name string) ([]map[string]string, error) {

	output, err := m.getJSONMetrics(client, ctx, bucket, name)
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]string, 0)
	for k, v := range output {

		if v.Value == "NaN" {
			continue
		}

		sv := strconv.FormatInt(v.Timestamp, 10)
		if sv == "NaN" {
			continue
		}

		row := map[string]string{"key": k, "value": v.Value, "timestamp": sv}
		rows = append(rows, row)
	}

	return rows, nil
}

type PrometheusData struct {
	ResultType string             `json:"resultType"`
	Result     []PrometheusMetric `json:"result"`
}

type PrometheusMetric struct {
	Metric PrometheusLabels `json:"metric"`
	Value  PrometheusValue  `json:"value"`
}

type PrometheusValue struct {
	Timestamp int64
	Value     string
}

type OutputMetric struct {
	Timestamp int64  `json:"timestamp"`
	Value     string `json:"value"`
}

type PrometheusResult struct {
	Status string         `json:"status"`
	Data   PrometheusData `json:"data"`
}

func (m *metricsLoader) getJSONMetrics(client *storage.Client, ctx context.Context, bucket, name string) (map[string]OutputMetric, error) {

	o := client.Bucket(bucket).Object(name)

	if m.instanceTime == nil {
		attrs, err := o.Attrs(ctx)
		if err != nil {
			return nil, err
		}

		if attrs.Created.IsZero() {
			return nil, fmt.Errorf("invalid creation time")
		}

		m.instanceTime = &attrs.Created
	}

	r, err := o.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	defer r.Close()

	// straight up lift from https://github.com/openshift/ci-search-functions/blob/master/functions.go#L191

	metrics := make(map[string]PrometheusResult)
	d := json.NewDecoder(r)
	var rows int
	for err = d.Decode(&metrics); err == nil; err = d.Decode(&metrics) {
		rows++
	}
	if err != nil && err != io.EOF {
		// we don't want to retry so log the error but return nil
		logwithctx(ctx).Errorf("failed to decode metric on line %d: %v", rows+1, err)
		return nil, nil
	}

	outputMetrics := make(map[string]OutputMetric, len(metrics))
	for name, v := range metrics {
		if v.Status != "success" {
			continue
		}
		if v.Data.ResultType != "vector" {
			continue
		}
		if len(v.Data.Result) == 0 {
			continue
		}
		if len(v.Data.Result) == 1 && len(v.Data.Result[0].Metric) == 0 {
			outputMetrics[name] = OutputMetric{
				Value:     v.Data.Result[0].Value.Value,
				Timestamp: v.Data.Result[0].Value.Timestamp,
			}
			// log.Printf("%s %s @ %d", name, v.Data.Result[0].Value.Value, v.Data.Result[0].Value.Timestamp)
			continue
		}
		var labels []string
		for i, result := range v.Data.Result {
			if len(labels) == 0 {
				labels = make([]string, len(result.Metric))
				j := 0
				for k := range result.Metric {
					labels[j] = k
					j++
				}
				if len(labels) == 0 {
					continue
				}
			}
			var metricSelector string
			for _, label := range labels {
				value, ok := result.Metric[label]
				if !ok {
					log.Printf("warn: Dropped result %d from %s because no value for metric %s", i, name, label)
					continue
				}
				if metricSelector != "" {
					metricSelector += ","
				}
				metricSelector += fmt.Sprintf("%s=%q", label, value)
			}
			outputMetrics[fmt.Sprintf("%s{%s}", name, metricSelector)] = OutputMetric{
				Value:     result.Value.Value,
				Timestamp: result.Value.Timestamp,
			}
			// log.Printf("%s{%s} %s @ %d", name, metricSelector, result.Value.Value, result.Value.Timestamp)
		}
	}

	_, ok := outputMetrics["job:duration:total:seconds"]
	if !ok {
		// we don't want to retry so log the error but return nil
		logwithctx(ctx).Errorf("job not indexed, does not have metric %q", "job:duration:total:seconds")
		return nil, nil
	}

	return outputMetrics, nil
}

// PrometheusLabels avoids deserialization allocations
type PrometheusLabels map[string]string

var _ json.Marshaler = PrometheusLabels(nil)
var _ json.Unmarshaler = &PrometheusLabels{}

func (l PrometheusLabels) MarshalJSON() ([]byte, error) {
	if len(l) == 0 {
		return []byte(`{}`), nil
	}
	return json.Marshal(map[string]string(l))
}

func (l *PrometheusLabels) UnmarshalJSON(data []byte) error {
	switch {
	case len(data) == 4 && bytes.Equal(data, []byte("null")):
		return nil
	case len(data) == 2 && bytes.Equal(data, []byte("{}")):
		if l == nil {
			return nil
		}
		for k := range *l {
			delete(*l, k)
		}
		return nil
	}
	if l == nil {
		*l = make(map[string]string)
	}
	var m *map[string]string = (*map[string]string)(l)
	return json.Unmarshal(data, m)
}

type parseState int

const (
	startState parseState = iota
	timestampState
	stringNumberState
	closeState
	doneState
)

func (l *PrometheusValue) UnmarshalJSON(data []byte) error {
	switch {
	case len(data) == 4 && bytes.Equal(data, []byte("null")):
		return nil
	case len(data) == 2 && bytes.Equal(data, []byte("[]")):
		return fmt.Errorf("unexpected value")
	}
	var state parseState = startState

	data = bytes.TrimSpace(data)
	for len(data) > 0 {
		switch data[0] {
		case '[':
			switch state {
			case startState:
				if l == nil {
					*l = PrometheusValue{}
				}
				data = bytes.TrimSpace(data[1:])
				state = timestampState
			default:
				return fmt.Errorf("unexpected character %c in state %d", data[0], state)
			}
		case ']':
			switch state {
			case closeState:
				data = bytes.TrimSpace(data[1:])
				state = doneState
			default:
				return fmt.Errorf("unexpected character %c in state %d", data[0], state)
			}
		default:
			switch state {
			case timestampState:
				pos := bytes.Index(data, []byte(","))
				if pos == -1 {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], could not find comma")
				}
				timestampBytes := bytes.TrimSpace(data[:pos])
				var err error
				l.Timestamp, err = strconv.ParseInt(string(timestampBytes), 10, 64)
				if err != nil {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], timestamp was not an int64: %v", err)
				}
				data = data[pos+1:]
				state = stringNumberState
			case stringNumberState:
				pos := bytes.Index(data, []byte("]"))
				if pos == -1 {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], could not find ending bracket in %q", string(data))
				}
				numberBytes := bytes.TrimSpace(data[:pos])
				if len(numberBytes) < 2 || numberBytes[0] != '"' || numberBytes[len(numberBytes)-1] != '"' {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], could not find number string")
				}
				b := numberBytes[1 : len(numberBytes)-1]
				if len(b) != len(bytes.TrimSpace(b)) {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], number was not a valid float64: whitespace in string")
				}
				s := string(b)
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					return fmt.Errorf("expected [<timestamp int>, \"<number string>\"], number was not a valid float64: %v", err)
				}
				l.Value = s
				data = data[pos:]
				state = closeState
			default:
				return fmt.Errorf("unexpected character %c in state %d", data[0], state)
			}
		}
	}
	if state != doneState {
		return fmt.Errorf("expected [<timestamp int>, \"<number string>\"]")
	}
	return nil
}
