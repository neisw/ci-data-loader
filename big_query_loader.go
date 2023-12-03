package cidataloader

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"k8s.io/apimachinery/pkg/util/wait"
	"net/http"
	"time"
)

const DataPartitioningField = "PartitionTime"
const JobRunNameField = "JobRunName"
const SourceNameField = "Source"

// Not using BigQueryDataCoordinates
// as we want this to migrate to a Cloud Function
// limiting use of existing types
type BigQueryLoader struct {
	ProjectID string
	DataSetID string
	Client    *bigquery.Client
	Table     *bigquery.Table
	DryRun    bool
}

type BigQueryDataItem struct {
	Instance *DataInstance
	Row      map[string]string
	InsertID string
}

var backoff = wait.Backoff{
	Duration: 10 * time.Second,
	Jitter:   0,
	Factor:   2,
	Steps:    3,
}

type ClientError struct {
	Err  error
	Call string
}

func (c *ClientError) Error() string {
	return fmt.Sprintf("client call %s: err %v", c.Call, c.Err)
}

func (b *BigQueryLoader) ValidateTable(ctx context.Context, dataInstance DataInstance) error {
	metaData, err := b.GetMetaData(dataInstance)
	if err != nil {
		return err
	}

	// if the table exists validate the metadata matches or update it
	table, err := b.validateBQTable(ctx, &metaData)
	if err != nil {
		return err
	}

	b.Table = table

	return nil
}

// validateTable can be called by multiple events at / around the same time
// leading to a race condition creating / updating the schema
// need to handle exponential backoff / retry in the event of create / update failures.
func (b *BigQueryLoader) validateBQTable(ctx context.Context, expectedMetaData *bigquery.TableMetadata) (*bigquery.Table, error) {
	// if it is a client error we will retry
	var table *bigquery.Table
	var validationErr error
	err := wait.ExponentialBackoff(backoff, func() (done bool, err error) {
		table, validationErr = b.retryableTableValidation(ctx, expectedMetaData)

		if validationErr != nil {
			if cerr, ok := err.(*ClientError); ok {
				return false, cerr
			}
		}
		return true, validationErr
	})

	if err != nil {
		return nil, validationErr
	}

	return table, nil
}
func (b *BigQueryLoader) retryableTableValidation(ctx context.Context, expectedMetaData *bigquery.TableMetadata) (*bigquery.Table, error) {
	dataSet := b.Client.Dataset(b.DataSetID)

	if dataSet == nil {
		return nil, fmt.Errorf("dataset (%s) does not exist", b.DataSetID)
	}

	table := dataSet.Table(expectedMetaData.Name)
	existingMetaData, err := table.Metadata(ctx)

	if err != nil {

		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == http.StatusNotFound {
				return b.createTable(ctx, expectedMetaData, table)
			}
		}

		return nil, err
	}

	return b.validateTableMetadata(ctx, expectedMetaData, existingMetaData, table)
}

func (b *BigQueryLoader) createTable(ctx context.Context, expectedMetaData *bigquery.TableMetadata, table *bigquery.Table) (*bigquery.Table, error) {
	err := table.Create(ctx, expectedMetaData)

	if err != nil {
		return nil, &ClientError{Err: err, Call: "Create"}
	}

	return table, nil
}

func (b *BigQueryLoader) validateTableMetadata(ctx context.Context, expectedMetaData *bigquery.TableMetadata, existingMetaData *bigquery.TableMetadata, table *bigquery.Table) (*bigquery.Table, error) {
	// validate our time partitioning field matches
	// we can update the TimePartitioning Type and Expiration if needed
	var updatedTimePartitioning *bigquery.TimePartitioning

	// not sure we can add / or change this...
	if existingMetaData.TimePartitioning == nil {

		// if we don't already have the required field or it doesn't match our requirements
		// then error out
		existingField := findField(existingMetaData.Schema, expectedMetaData.TimePartitioning.Field)
		if existingField == nil {
			return nil, fmt.Errorf("Missing time partitioned field in the existing schema: %s", expectedMetaData.TimePartitioning.Field)
		}

		expectedField := findField(expectedMetaData.Schema, expectedMetaData.TimePartitioning.Field)
		// this would be a problem in our code...
		if expectedField == nil {
			return nil, fmt.Errorf("Missing time partitioned field in the expected schema: %s", expectedMetaData.TimePartitioning.Field)
		}

		if expectedField.Required != existingField.Required {
			return nil, fmt.Errorf("Existing time partition field required status does not match.  Expected: %v, Existing: %v", expectedField.Required, existingField.Required)
		}

		if expectedField.Type != existingField.Type {
			return nil, fmt.Errorf("Existing time partition field type does not match.  Expected: %v, Existing: %v", expectedField.Type, existingField.Type)
		}

		updatedTimePartitioning = expectedMetaData.TimePartitioning
	}

	// check to see if our schema has any columns that don't currently exist
	// if so then we add them keeping all that already exist, we don't currently drop columns
	missingFields := make([]*bigquery.FieldSchema, 0)

	for _, expectedField := range expectedMetaData.Schema {
		existingField := findField(existingMetaData.Schema, expectedField.Name)

		// ok if existing is required and expected isn't
		// but not ok if expected is required and existing isn't
		if expectedField.Required {
			if existingField == nil {
				return nil, fmt.Errorf("field %s is required but is missing in the existing schema", expectedField.Name)
			}
			if !existingField.Required {
				return nil, fmt.Errorf("field %s is required but optional in the existing schema", expectedField.Name)
			}
		}

		if existingField == nil {
			missingFields = append(missingFields, expectedField)
		} else if expectedField.Type != existingField.Type {
			return nil, fmt.Errorf("field %s expected type: %s does not match existing schema type: %s", expectedField.Name, expectedField.Type, existingField.Type)
		}
	}

	if len(missingFields) > 0 || updatedTimePartitioning != nil {
		// something to update so lets try
		update := bigquery.TableMetadataToUpdate{
			TimePartitioning: updatedTimePartitioning,
		}

		if len(missingFields) > 0 {
			update.Schema = append(existingMetaData.Schema, missingFields...)
		}

		// handle back off / retry

		_, err := table.Update(ctx, update, existingMetaData.ETag)
		if err != nil {
			return nil, &ClientError{Err: err, Call: "Update"}
		}

	}
	return table, nil

}

func (b *BigQueryLoader) DeleteExistingData(ctx context.Context, partitionTime time.Time, partitionColumn, tableName, jobRunName, source string) error {

	deleteFromDate := partitionTime.Add(-2 * 24 * time.Hour)

	deleteQuery := fmt.Sprintf("DELETE FROM `%s.%s` WHERE %s > TIMESTAMP(\"%s\") AND %s = \"%s\" AND %s = \"%s\"", b.DataSetID, tableName, partitionColumn, deleteFromDate.Format(time.RFC3339), JobRunNameField, jobRunName, SourceNameField, source)

	q := b.Client.Query(deleteQuery)

	// q.DryRun = true
	job, err := q.Run(ctx)

	if err != nil {
		return err
	}

	js, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if js.Err() != nil {
		return js.Err()
	}

	// more checks for js?

	return nil
}

func (b *BigQueryLoader) FindExistingData(ctx context.Context, partitionTime time.Time, partitionColumn, tableName, jobRunName, source string) (uint64, error) {

	countFromDate := partitionTime.Add(-2 * 24 * time.Hour)

	countQuery := fmt.Sprintf("SELECT COUNT(*) AS TotalRows FROM `%s.%s` WHERE `%s` > TIMESTAMP(\"%s\") AND %s = \"%s\" AND %s = \"%s\"", b.DataSetID, tableName, partitionColumn, countFromDate.Format(time.RFC3339), JobRunNameField, jobRunName, SourceNameField, source)

	query := b.Client.Query(countQuery)

	var rowCount struct {
		TotalRows int64
	}
	it, err := query.Read(ctx)
	if err != nil {
		return 0, err
	}

	err = it.Next(&rowCount)
	if err != nil {
		return 0, err
	}

	return uint64(rowCount.TotalRows), nil
}

func findField(schema bigquery.Schema, name string) *bigquery.FieldSchema {
	for _, field := range schema {
		if field.Name == name {
			return field
		}
	}
	return nil
}

func (b *BigQueryLoader) GetMetaData(di DataInstance) (bigquery.TableMetadata, error) {

	var timePartitioningType bigquery.TimePartitioningType
	switch bigquery.TimePartitioningType(di.DataFile.PartitionType) {
	case bigquery.HourPartitioningType:
		timePartitioningType = bigquery.HourPartitioningType
	case bigquery.MonthPartitioningType:
		timePartitioningType = bigquery.MonthPartitioningType
	case bigquery.YearPartitioningType:
		timePartitioningType = bigquery.YearPartitioningType
	default:
		timePartitioningType = bigquery.DayPartitioningType
	}

	if len(timePartitioningType) == 0 {
		timePartitioningType = bigquery.DayPartitioningType
	}

	expiration := 365

	// need a warning in our validation if we won't honor the expiration
	if di.DataFile.ExpirationDays > 0 && di.DataFile.ExpirationDays < 2*expiration {
		expiration = di.DataFile.ExpirationDays
	} else if di.DataFile.ExpirationDays > 0 {
		logrus.Warnf("Expiration days out of range: %d", di.DataFile.ExpirationDays)
	}

	schema := bigquery.Schema{}

	// add in dynamic fields, and then any defaults
	schema = b.getBQSchema(di, schema)

	// check to see if we have an existing metadata definition
	// if so then we have to honor the exising partition settings
	// if not then we check to see if any partition settings have been passed in
	// otherwise use our defaults
	var partitionColumn *bigquery.FieldSchema
	if len(di.DataFile.PartitionColumn) > 0 {
		for _, dfColumn := range schema {
			if dfColumn.Name == di.DataFile.PartitionColumn {
				if dfColumn.Type == bigquery.TimestampFieldType {
					dfColumn.Required = true // won't be true by default so set it here
					partitionColumn = &bigquery.FieldSchema{}
				}
			}
		}
	}

	if partitionColumn == nil {
		partitionColumn = &bigquery.FieldSchema{Name: DataPartitioningField, Type: bigquery.TimestampFieldType, Required: true}
		// update the datafile to reflect
		di.DataFile.PartitionColumn = partitionColumn.Name
		schema = append(schema, partitionColumn)
	}

	schema = append(schema, &bigquery.FieldSchema{Name: JobRunNameField, Type: bigquery.StringFieldType, Required: true})
	schema = append(schema, &bigquery.FieldSchema{Name: SourceNameField, Type: bigquery.StringFieldType, Required: true})

	metaData := bigquery.TableMetadata{Name: di.DataFile.TableName,
		TimePartitioning: &bigquery.TimePartitioning{Type: timePartitioningType, Expiration: time.Hour * time.Duration(24*expiration), Field: di.DataFile.PartitionColumn},
		Schema:           schema,
	}

	return metaData, nil
}

func (b *BigQueryLoader) getBQSchema(di DataInstance, schema bigquery.Schema) bigquery.Schema {

	for k, v := range di.DataFile.Schema {

		var bqFieldType bigquery.FieldType

		switch v {
		case DataTypeFloat64:
			bqFieldType = bigquery.FloatFieldType
		case DataTypeJSON:
			bqFieldType = bigquery.JSONFieldType
		case DataTypeTimestamp:
			bqFieldType = bigquery.TimestampFieldType
		case DataTypeString:
			bqFieldType = bigquery.StringFieldType
		case DataTypeInteger:
			bqFieldType = bigquery.IntegerFieldType
		default:
			logrus.Warnf("Unknown field type detected: %s", v)
			continue
		}
		schema = append(schema, &bigquery.FieldSchema{Name: k, Type: bqFieldType, Required: false})
	}

	return schema
}

func (b *BigQueryLoader) LoadDataItems(ctx context.Context, dataInstance DataInstance) ([]interface{}, error) {

	var inserter *bigquery.Inserter
	if !b.DryRun {
		inserter = b.Table.Inserter()
	}
	// JobRunName and CreationTime are required
	if len(dataInstance.JobRunName) == 0 {
		return nil, fmt.Errorf("missing Job run name")
	}

	if dataInstance.CreationTime.IsZero() {
		return nil, fmt.Errorf("missing creation time")
	}

	diRows := make([]interface{}, 0)

	chunkSize := dataInstance.DataFile.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 999
	}
	for i, r := range dataInstance.DataFile.Rows {

		diRows = append(diRows, bigquery.ValueSaver(&BigQueryDataItem{Instance: &dataInstance, Row: r, InsertID: fmt.Sprintf("%s-%d", dataInstance.JobRunName, i)}))

		if len(diRows) > chunkSize {
			if inserter != nil {
				insertRows(ctx, inserter, diRows)
				diRows = make([]interface{}, 0)
			}
		}
	}

	if len(diRows) > 0 && inserter != nil {
		insertRows(ctx, inserter, diRows)
	} else {
		return diRows, nil
	}

	return nil, nil
}

func insertRows(ctx context.Context, inserter *bigquery.Inserter, items []interface{}) {
	// add in backoff / retry handling
	err := inserter.Put(ctx, items)

	// do something more with the error?
	if err != nil {
		logrus.Errorf("Error inserting records: %v", err)
	}
}

// When implementing the overall save, need to consider the total number of rows / data size
// and chunk accordingly
// One detail on the intervals upload -
// these files can be larger than 10MB, which is the limit for a BigQuery insert.
// Depending on your implementation, you will probably want to chunk the records into 1000 or so blocks for each insert.

// Save implements the ValueSaver interface.
func (i *BigQueryDataItem) Save() (row map[string]bigquery.Value, insertID string, err error) {

	// we need a data item which references the data loader
	// for the values in the item we call the key and value mappers
	row = make(map[string]bigquery.Value, len(i.Row))

	// set our defaults
	row[JobRunNameField] = i.Instance.JobRunName
	row[SourceNameField] = i.Instance.Source

	foundPartionValue := false
	for k, v := range i.Row {
		bv, err := i.Instance.valueMapper(k, v)
		if err != nil {
			return nil, "", err
		}
		rowKey := i.Instance.keyMapper(k)
		// skip over unknown keys
		if _, ok := i.Instance.DataFile.Schema[rowKey]; !ok {
			continue
		}
		row[rowKey] = bv
		if rowKey == i.Instance.DataFile.PartitionColumn {
			foundPartionValue = true
		}
	}

	// we need to know if we are providing this or it is coming from an existing field
	if !foundPartionValue {
		if i.Instance.DataFile.PartitionColumn == DataPartitioningField {
			row[DataPartitioningField] = i.Instance.CreationTime
		} else {
			// we don't have a valid partition value
			return nil, "", fmt.Errorf("missing value for partition column: %s, %v", i.Instance.DataFile.PartitionColumn, row)
		}
	}

	return row, i.InsertID, nil
}
