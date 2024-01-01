package cidataloader

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

type DataType = string

const (
	DataTypeFloat64 DataType = "float64"
	DataTypeString  DataType = "string"
	DataTypeInteger DataType = "int64"
	// RFC3339  based value "2006-01-02T15:04:05Z07:00
	DataTypeTimestamp DataType = "timestamp"
	DataTypeJSON      DataType = "json"
	DataTypeBool      DataType = "bool"
)

type DataFile struct {
	TableName       string              `json:"table_name"`
	Schema          map[string]DataType `json:"schema"`
	SchemaMapping   map[string]string   `json:"schema_mapping"`
	Rows            []map[string]string `json:"rows"`
	ExpirationDays  int                 `json:"expiration_days"`
	PartitionColumn string              `json:"partition_column"`
	PartitionType   string              `json:"partition_type"`
	ChunkSize       int                 `json:"chunk_size"`
}

type DataInstance struct {
	Source       string
	CreationTime time.Time
	JobRunName   string
	DataFile     *DataFile
}

func generateDataFileUploader(client *storage.Client, ctx context.Context, event *JobRunDataEvent, dataLoader DataLoader) (SimpleUploader, error) {
	o := client.Bucket(event.GCSEvent.Bucket).Object(event.GCSEvent.Name)

	creationTime := event.GCSEvent.TimeCreated
	if creationTime.IsZero() {
		attrs, err := o.Attrs(ctx)
		if err != nil {
			return nil, err
		}

		if attrs.Created.IsZero() {
			return nil, fmt.Errorf("invalid creation time")
		}

		creationTime = attrs.Created
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

	var df DataFile
	err = UnMarshalJSON(b, &df)
	if err != nil {
		return nil, err
	}

	// then a data Instance
	dataInstance := DataInstance{JobRunName: event.Job, CreationTime: creationTime, Source: event.Filename, DataFile: &df}

	loader, err := generateDataUploader(dataInstance, dataLoader)
	if err != nil {
		return nil, err
	}

	return loader, nil
}

func UnMarshalJSON(jsonb []byte, result interface{}) error {
	// TODO: research using https://github.com/goccy/go-json
	err := json.Unmarshal(jsonb, result)
	if err != nil {
		return fmt.Errorf("cannot decode JSON: %v", err)
	}
	return nil
}

func (d *DataInstance) keyMapper(key string) string {

	if len(d.DataFile.SchemaMapping) > 0 {
		km, ok := d.DataFile.SchemaMapping[key]

		if ok {
			return km
		}
	}

	return key
}

func (d *DataInstance) valueMapper(key, value string) (any, error) {

	// need to convert empty to 0 at all?
	if len(value) == 0 {
		return nil, nil
	}

	// we have to have a schema, panic if we don't
	dt, ok := d.DataFile.Schema[key]

	if ok {
		switch dt {
		case DataTypeFloat64:
			return strconv.ParseFloat(value, 64)
		case DataTypeString:
			return value, nil
		case DataTypeJSON:
			return value, nil
		case DataTypeInteger:
			return strconv.ParseInt(value, 10, 64)
		case DataTypeTimestamp:
			return time.Parse(time.RFC3339, value)
		case DataTypeBool:
			return strconv.ParseBool(value)
		}
	}

	return value, nil
}
