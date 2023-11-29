package cidataloader

import (
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBigQueryLoaderRows(t *testing.T) {

	modifiedTimeString := "2023-10-17T07:05:07Z"
	modifiedTime, err := time.Parse(time.RFC3339, modifiedTimeString)
	if err != nil {
		t.Fatalf("Failed to parse time: %s", modifiedTimeString)
	}

	tests := []struct {
		name            string
		dataInstance    DataInstance
		expectedResults []map[string]bigquery.Value
		expectedError   bool
	}{
		{
			name: "test_simple",
			dataInstance: DataInstance{JobRunName: "1604867803796475904",
				CreationTime: modifiedTime,
				Source:       "testSource",
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: DataPartitioningField,
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0"}, {"string1": "string1_val2.1", "float1": "2.1"}},
				},
			},
			expectedResults: []map[string]bigquery.Value{{"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "string1": "string1_val1.0", "float1": float64(1)}, {"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "string1": "string1_val2.1", "float1": float64(2.1)}},
		},
		{
			name: "test_partition_time",
			dataInstance: DataInstance{JobRunName: "1604867803796475904",
				CreationTime: modifiedTime,
				Source:       "testSource",
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: "overridePartition",
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64, "overridePartition": DataTypeTimestamp},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0", "overridePartition": modifiedTimeString}, {"string1": "string1_val2.1", "float1": "2.1", "overridePartition": modifiedTimeString}},
				},
			},
			expectedResults: []map[string]bigquery.Value{{"JobRunName": "1604867803796475904", "overridePartition": modifiedTime, "Source": "testSource", "string1": "string1_val1.0", "float1": float64(1)}, {"JobRunName": "1604867803796475904", "overridePartition": modifiedTime, "Source": "testSource", "string1": "string1_val2.1", "float1": float64(2.1)}},
		},
		{
			name: "test_partition_time_missing",
			dataInstance: DataInstance{JobRunName: "1604867803796475904",
				CreationTime: modifiedTime,
				Source:       "testSource",
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: "overridePartition",
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64, "overridePartition": DataTypeTimestamp},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0"}, {"string1": "string1_val2.1", "float1": "2.1", "overridePartition": modifiedTimeString}},
				},
			},
			expectedError:   true,
			expectedResults: []map[string]bigquery.Value{nil, {"JobRunName": "1604867803796475904", "overridePartition": modifiedTime, "Source": "testSource", "string1": "string1_val2.1", "float1": float64(2.1)}},
		},
		{
			name: "test_json",
			dataInstance: DataInstance{JobRunName: "1604867803796475904",
				CreationTime: modifiedTime,
				Source:       "testSource",
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: DataPartitioningField,
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64, "json1": DataTypeJSON},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0", "json1": `{"Name":"NameValue", "Field": "FieldValue"}`}, {"string1": "string1_val2.1", "float1": "2.1", "json1": `{"Name":"NameValue2", "Field": "FieldValue2"}`}},
				},
			},
			expectedResults: []map[string]bigquery.Value{{"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "string1": "string1_val1.0", "float1": float64(1), "json1": `{"Name":"NameValue", "Field": "FieldValue"}`},
				{"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "string1": "string1_val2.1", "float1": float64(2.1), "json1": `{"Name":"NameValue2", "Field": "FieldValue2"}`},
			},
		},
		{
			name: "test_mapping",
			dataInstance: DataInstance{JobRunName: "1604867803796475904",
				CreationTime: modifiedTime,
				Source:       "testSource",
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: DataPartitioningField,
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64},
					SchemaMapping:   map[string]string{"string1": "table_string_1"},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0"}, {"string1": "string1_val2.1", "float1": "2.1"}},
				},
			},
			expectedResults: []map[string]bigquery.Value{{"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "table_string_1": "string1_val1.0", "float1": float64(1)}, {"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "table_string_1": "string1_val2.1", "float1": float64(2.1)}},
		},
		{
			name: "test_exception",
			dataInstance: DataInstance{JobRunName: "1604867803796475904",
				CreationTime: modifiedTime,
				Source:       "testSource",
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: DataPartitioningField,
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64},
					SchemaMapping:   map[string]string{"string1": "table_string_1"},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0 with some text"}, {"string1": "string1_val2.1", "float1": "2.1"}},
				},
			},
			expectedResults: []map[string]bigquery.Value{nil, {"JobRunName": "1604867803796475904", "PartitionTime": modifiedTime, "Source": "testSource", "table_string_1": "string1_val2.1", "float1": float64(2.1)}},
			expectedError:   true,
		},
	}

	bigQueryLoader := BigQueryLoader{DryRun: true}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataItems, err := bigQueryLoader.LoadDataItems(context.TODO(), test.dataInstance)

			assert.Nil(t, err, "%s: Unexpected error %v", test.name, err)

			assert.NotNil(t, dataItems, "%s: Nil data items", test.name)

			matchResults := make([]map[string]bigquery.Value, len(dataItems))

			for i, r := range dataItems {
				bqv, _, err := r.(bigquery.ValueSaver).Save()

				if test.expectedError && err != nil {
					// we will use the matchResults
					continue
				}

				assert.Nil(t, err, "%s: Unexpected item error %v", test.name, err)
				assert.NotNil(t, bqv, "%s: Invalid big query Row", test.name)

				matchResults[i] = bqv
			}

			assert.Equal(t, test.expectedResults, matchResults)

		})
	}

}

func TestBigQueryLoaderMetaData(t *testing.T) {

	modifiedTimeString := "2023-10-17T07:05:07Z"
	tests := []struct {
		name            string
		dataInstance    DataInstance
		expectedResults bigquery.TableMetadata
		expectedError   bool
	}{
		{
			name: "test_simple",
			dataInstance: DataInstance{
				DataFile: &DataFile{
					TableName: "test_table",
					Schema:    map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64},
					Rows:      []map[string]string{{"string1": "string1_val1.0", "float1": "1.0"}, {"string1": "string1_val2.1", "float1": "2.1"}},
				},
			},
			expectedResults: bigquery.TableMetadata{
				Name: "test_table",
				Schema: bigquery.Schema{&bigquery.FieldSchema{Name: DataPartitioningField, Type: bigquery.TimestampFieldType, Required: true},
					&bigquery.FieldSchema{Name: JobRunNameField, Type: bigquery.StringFieldType, Required: true},
					&bigquery.FieldSchema{Name: SourceNameField, Type: bigquery.StringFieldType, Required: true},
					&bigquery.FieldSchema{Name: "string1", Type: bigquery.StringFieldType, Required: false},
					&bigquery.FieldSchema{Name: "float1", Type: bigquery.FloatFieldType, Required: false},
				},
				TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Expiration: time.Hour * time.Duration(24*365), Field: DataPartitioningField},
			},
		},
		{
			name: "test_json",
			dataInstance: DataInstance{
				DataFile: &DataFile{
					TableName: "test_table",
					Schema:    map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64, "json1": DataTypeJSON},
					Rows:      []map[string]string{{"string1": "string1_val1.0", "float1": "1.0", "json1": `{"Name":"NameValue1", "Field": "FieldValue1"}`}, {"string1": "string1_val2.1", "float1": "2.1", "json1": `{"Name":"NameValue2", "Field": "FieldValue2"}`}},
				},
			},
			expectedResults: bigquery.TableMetadata{
				Name: "test_table",
				Schema: bigquery.Schema{&bigquery.FieldSchema{Name: DataPartitioningField, Type: bigquery.TimestampFieldType, Required: true},
					&bigquery.FieldSchema{Name: JobRunNameField, Type: bigquery.StringFieldType, Required: true},
					&bigquery.FieldSchema{Name: SourceNameField, Type: bigquery.StringFieldType, Required: true},
					&bigquery.FieldSchema{Name: "string1", Type: bigquery.StringFieldType, Required: false},
					&bigquery.FieldSchema{Name: "float1", Type: bigquery.FloatFieldType, Required: false},
					&bigquery.FieldSchema{Name: "json1", Type: bigquery.JSONFieldType, Required: false},
				},
				TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Expiration: time.Hour * time.Duration(24*365), Field: DataPartitioningField},
			},
		},
		{
			name: "test_override_partition",
			dataInstance: DataInstance{
				DataFile: &DataFile{
					TableName:       "test_table",
					PartitionColumn: "overridePartition",
					Schema:          map[string]DataType{"string1": DataTypeString, "float1": DataTypeFloat64, "json1": DataTypeJSON, "overridePartition": DataTypeTimestamp},
					Rows:            []map[string]string{{"string1": "string1_val1.0", "float1": "1.0", "overridePartition": modifiedTimeString, "json1": `{"Name":"NameValue1", "Field": "FieldValue1"}`}, {"string1": "string1_val2.1", "float1": "2.1", "overridePartition": modifiedTimeString, "json1": `{"Name":"NameValue2", "Field": "FieldValue2"}`}},
				},
			},
			expectedResults: bigquery.TableMetadata{
				Name: "test_table",
				Schema: bigquery.Schema{&bigquery.FieldSchema{Name: "overridePartition", Type: bigquery.TimestampFieldType, Required: true},
					&bigquery.FieldSchema{Name: JobRunNameField, Type: bigquery.StringFieldType, Required: true},
					&bigquery.FieldSchema{Name: SourceNameField, Type: bigquery.StringFieldType, Required: true},
					&bigquery.FieldSchema{Name: "string1", Type: bigquery.StringFieldType, Required: false},
					&bigquery.FieldSchema{Name: "float1", Type: bigquery.FloatFieldType, Required: false},
					&bigquery.FieldSchema{Name: "json1", Type: bigquery.JSONFieldType, Required: false},
				},
				TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Expiration: time.Hour * time.Duration(24*365), Field: "overridePartition"},
			},
		},
	}

	bigQueryLoader := BigQueryLoader{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metaData, err := bigQueryLoader.GetMetaData(test.dataInstance)

			assert.Nil(t, err, "%s: Unexpected error %v", test.name, err)
			assert.NotNil(t, metaData)

			// won't work with the pointers, need to validate individual pieces
			// assert.Equal(t, test.expectedResults, metaData)

			assert.Equal(t, test.expectedResults.Name, metaData.Name)
			assert.Equal(t, test.expectedResults.TimePartitioning.Type, metaData.TimePartitioning.Type)
			assert.Equal(t, test.expectedResults.TimePartitioning.Field, metaData.TimePartitioning.Field)
			assert.Equal(t, test.expectedResults.TimePartitioning.Expiration, metaData.TimePartitioning.Expiration)
			assert.Equal(t, len(test.expectedResults.Schema), len(metaData.Schema), "Schema lengths did not match")

			for _, expectedField := range test.expectedResults.Schema {
				matchField := findField(metaData.Schema, expectedField.Name)
				assert.NotNil(t, matchField, "missing schema field: %s", expectedField.Name)
				assert.Equal(t, expectedField.Type, matchField.Type)
				assert.Equal(t, expectedField.Required, matchField.Required)
			}

		})
	}

}
