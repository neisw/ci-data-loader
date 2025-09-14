package cidataloader

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"k8s.io/apimachinery/pkg/util/wait"
	"net/http"
	"strings"
	"time"
)

const DataPartitioningField = "PartitionTime"
const JobRunNameField = "JobRunName"
const SourceNameField = "Source"
const internalWriteLog = "internal_write_log"
const modifiedTime = "ModifiedTime"
const internalWriteLogCompleted = "Completed"
const maxRetries = 5

type BigQueryLoader struct {
	ProjectID  string
	DataSetID  string
	Client     *bigquery.Client
	DryRun     bool
	tableCache map[string]BigQueryTableCache
}

type BigQueryTableCache struct {
	metaData  *bigquery.TableMetadata
	table     *bigquery.Table
	cacheTime time.Time
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

func (b *BigQueryLoader) Init(ctx context.Context) error {
	// we don't need the instance other than to host the dataFile
	dataInstance := DataInstance{CreationTime: time.Now(), JobRunName: "startup", Source: "internal"}
	dataFile := DataFile{
		TableName: internalWriteLog,
		Schema:    map[string]DataType{internalWriteLogCompleted: DataTypeBool, modifiedTime: DataTypeTimestamp},
		ChunkSize: 1000,
	}

	dataInstance.DataFile = &dataFile

	return b.ValidateTable(ctx, dataInstance)

	return nil
}

func (b *BigQueryLoader) ValidateTable(ctx context.Context, dataInstance DataInstance) error {
	metaData, err := b.GetMetaData(ctx, dataInstance)
	if err != nil {
		return err
	}

	// if the table exists validate the metadata matches or update it
	err = b.validateBQTable(ctx, &metaData)
	if err != nil {
		return err
	}

	return nil
}

// validateBQTable can be called by multiple events at / around the same time
// leading to a race condition creating / updating the schema
// need to handle exponential backoff / retry in the event of create / update failures.
func (b *BigQueryLoader) validateBQTable(ctx context.Context, expectedMetaData *bigquery.TableMetadata) error {
	// if it is a client error we will retry
	var validationErr error
	err := wait.ExponentialBackoff(backoff, func() (done bool, err error) {
		validationErr = b.retryableTableValidation(ctx, expectedMetaData, false)

		if validationErr != nil {
			if cerr, ok := err.(*ClientError); ok {
				return false, cerr
			}
		}
		return true, validationErr
	})

	if err != nil {
		return validationErr
	}

	return nil
}
func (b *BigQueryLoader) retryableTableValidation(ctx context.Context, expectedMetaData *bigquery.TableMetadata, skipValidateMetadata bool) error {
	dataSet := b.Client.Dataset(b.DataSetID)

	if dataSet == nil {
		return fmt.Errorf("dataset (%s) does not exist", b.DataSetID)
	}

	// see if we have already fetched the metadata
	// if so, see if it matches
	// if not, then do a lookup and check again
	// if it still doesn't match then try the migration
	if b.tableCache == nil {
		b.tableCache = make(map[string]BigQueryTableCache)
	}

	cachedTable, ok := b.tableCache[expectedMetaData.Name]

	if ok {
		// if we cached the table more than an hour ago let it fall through
		// so we look it up again
		if !skipValidateMetadata && cachedTable.table != nil && cachedTable.metaData != nil && cachedTable.cacheTime.After(time.Now().Add(-1*time.Hour)) {
			hasUpdate, err := b.validateTableMetadata(ctx, expectedMetaData, cachedTable.metaData, cachedTable.table, true)
			if err != nil {
				return err
			}
			// no changes between our cached metaData and the incoming metaData so skip the lookup and
			// return
			if !hasUpdate {
				return nil
			}
		}
	}

	// if we reach this point then invalidate any cache we have for this table
	delete(b.tableCache, expectedMetaData.Name)

	table := dataSet.Table(expectedMetaData.Name)
	existingMetaData, err := table.Metadata(ctx)

	if err != nil {

		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == http.StatusNotFound {
				return b.createTable(ctx, expectedMetaData, table)
			}
		}

		return err
	}

	if !skipValidateMetadata {
		hasUpdate, err := b.validateTableMetadata(ctx, expectedMetaData, existingMetaData, table, false)

		if err != nil {
			return err
		}

		if hasUpdate {
			// get the current metadata
			// we have to look it up again if we
			// just changed it
			existingMetaData, err = table.Metadata(ctx)

			if err != nil {
				return err
			}
		}
	}

	// store our most recent version in the cache
	cachedTable = BigQueryTableCache{table: table, metaData: existingMetaData, cacheTime: time.Now()}
	b.tableCache[expectedMetaData.Name] = cachedTable

	return nil
}

func (b *BigQueryLoader) retryableTableCache(ctx context.Context, expectedMetaData *bigquery.TableMetadata) error {

	// if it is a client error we will retry
	var validationErr error
	err := wait.ExponentialBackoff(backoff, func() (done bool, err error) {
		validationErr = b.retryableTableValidation(ctx, expectedMetaData, true)

		if validationErr != nil {
			if cerr, ok := err.(*ClientError); ok {
				return false, cerr
			}
		}
		return true, validationErr
	})

	if err != nil {
		return validationErr
	}

	return nil

}

func (b *BigQueryLoader) createTable(ctx context.Context, expectedMetaData *bigquery.TableMetadata, table *bigquery.Table) error {
	err := table.Create(ctx, expectedMetaData)

	if err != nil {
		return &ClientError{Err: err, Call: "Create"}
	}

	// update our cache with the newly created table data
	b.tableCache[expectedMetaData.Name] = BigQueryTableCache{metaData: expectedMetaData, table: table, cacheTime: time.Now()}

	return nil
}

func (b *BigQueryLoader) validateTableMetadata(ctx context.Context, expectedMetaData *bigquery.TableMetadata, existingMetaData *bigquery.TableMetadata, table *bigquery.Table, dryRun bool) (bool, error) {
	// validate our time partitioning field matches

	// we do not support changing the Partitioning column

	//	not going to change anything about the partitioning
	//	make sure the field is in the schema is all
	//	report warnings if the existing and expected differ

	// if existingMetaData.TimePartitioning == nil {
	//
	// 	// if we don't already have the required field or it doesn't match our requirements
	// 	// then error out
	// 	existingField := findField(existingMetaData.Schema, expectedMetaData.TimePartitioning.Field)
	// 	if existingField == nil {
	// 		return false, fmt.Errorf("missing time partitioned field in the existing schema: %s", expectedMetaData.TimePartitioning.Field)
	// 	}
	//
	// 	expectedField := findField(expectedMetaData.Schema, expectedMetaData.TimePartitioning.Field)
	// 	// this would be a problem in our code...
	// 	if expectedField == nil {
	// 		return false, fmt.Errorf("missing time partitioned field in the expected schema: %s", expectedMetaData.TimePartitioning.Field)
	// 	}
	//
	// 	if expectedField.Required != existingField.Required {
	// 		return false, fmt.Errorf("existing time partition field required status does not match.  Expected: %v, Existing: %v", expectedField.Required, existingField.Required)
	// 	}
	//
	// 	if expectedField.Type != existingField.Type {
	// 		return false, fmt.Errorf("existing time partition field type does not match.  Expected: %v, Existing: %v", expectedField.Type, existingField.Type)
	// 	}
	//
	// 	updatedTimePartitioning = expectedMetaData.TimePartitioning
	// } else {
	// 	if existingMetaData.TimePartitioning.Field != expectedMetaData.TimePartitioning.Field {
	// 		return false, fmt.Errorf("existing time partition field '%s' does not match expected field: %s", existingMetaData.TimePartitioning.Field, expectedMetaData.TimePartitioning.Field)
	// 	}
	//
	// 	if expectedMetaData.TimePartitioning.Type != expectedMetaData.TimePartitioning.Type {
	// 		if updatedTimePartitioning == nil {
	// 			updatedTimePartitioning = &bigquery.TimePartitioning{Type: expectedMetaData.TimePartitioning.Type}
	// 		}
	// 	}
	//
	// 	change expiration , yes if allowed
	// }

	// check to see if our schema has any columns that don't currently exist
	// if so then we add them keeping all that already exist, we don't currently drop columns
	missingFields := make([]*bigquery.FieldSchema, 0)

	for _, expectedField := range expectedMetaData.Schema {
		existingField := findField(existingMetaData.Schema, expectedField.Name)

		// ok if existing is required and expected isn't
		// but not ok if expected is required and existing isn't
		if expectedField.Required {
			if existingField == nil {
				return false, fmt.Errorf("field %s is required but is missing in the existing schema", expectedField.Name)
			}
			if !existingField.Required {
				return false, fmt.Errorf("field %s is required but optional in the existing schema", expectedField.Name)
			}
		}

		if existingField == nil {
			missingFields = append(missingFields, expectedField)
		} else if expectedField.Type != existingField.Type {
			return false, fmt.Errorf("field %s expected type: %s does not match existing schema type: %s", expectedField.Name, expectedField.Type, existingField.Type)
		}
	}

	if len(missingFields) > 0 { // || updatedTimePartitioning != nil {

		// if we are in dryRun mode and the schema has changed then return true indicating the schema has changes
		if dryRun {
			return true, nil
		}

		// something to update so lets try
		update := bigquery.TableMetadataToUpdate{}

		if len(missingFields) > 0 {
			update.Schema = append(existingMetaData.Schema, missingFields...)
		}

		if table == nil {
			return false, fmt.Errorf("missing table for: %s", existingMetaData.Name)
		}

		_, err := table.Update(ctx, update, existingMetaData.ETag)
		if err != nil {
			return false, &ClientError{Err: err, Call: "Update"}
		}
		return true, nil
	}

	return false, nil
}

func (b *BigQueryLoader) FindExistingData(ctx context.Context, partitionTime time.Time, partitionColumn, tableName, jobRunName, source string) (bool, error) {

	// first check for an entry in our write log table, if the entry is marked complete then return true
	// if the entry doesn't exist then return false
	// if the entry exists but isn't marked complete see if we have any data in the actual table

	countFromDate := partitionTime.Add(-2 * 24 * time.Hour)
	countToDate := partitionTime.Add(2 * 24 * time.Hour)

	countCompletedQuery := fmt.Sprintf("SELECT TableTemp.TotalRows, TableTemp.CompletedRows FROM ((SELECT(SELECT COUNT(*) FROM `%s.%s` WHERE `%s` > TIMESTAMP(\"%s\") AND `%s` < TIMESTAMP(\"%s\") AND %s = \"%s\" AND %s = \"%s\") AS TotalRows, (SELECT(SELECT COUNT(*) FROM `%s.%s` WHERE `%s` > TIMESTAMP(\"%s\") AND `%s` < TIMESTAMP(\"%s\") AND %s = \"%s\" AND %s = \"%s\" AND %s IS TRUE) ) AS CompletedRows )) AS TableTemp", b.DataSetID, internalWriteLog, DataPartitioningField, countFromDate.Format(time.RFC3339), DataPartitioningField, countToDate.Format(time.RFC3339), JobRunNameField, jobRunName, SourceNameField, source, b.DataSetID, internalWriteLog, DataPartitioningField, countFromDate.Format(time.RFC3339), DataPartitioningField, countToDate.Format(time.RFC3339), JobRunNameField, jobRunName, SourceNameField, source, internalWriteLogCompleted)

	rowCount, completedCount, err := b.readRowCounts(ctx, countCompletedQuery)

	if err == nil {
		// if the query completed and we have no record then we have no record of it
		if rowCount < 1 {
			return false, nil
		}

		// if we have a row count and completedCount is < 1 then we will fall through and check for existing data
		if completedCount > 0 {
			return true, nil
		}

	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) AS TotalRows FROM `%s.%s` WHERE `%s` > TIMESTAMP(\"%s\") AND `%s` < TIMESTAMP(\"%s\") AND %s = \"%s\" AND %s = \"%s\"", b.DataSetID, tableName, partitionColumn, countFromDate.Format(time.RFC3339), partitionColumn, countToDate.Format(time.RFC3339), JobRunNameField, jobRunName, SourceNameField, source)

	rowCount, _, err = b.readRowCounts(ctx, countQuery)

	if err != nil {
		return false, err
	}

	return rowCount > 0, nil
}

func (b *BigQueryLoader) readRowCounts(ctx context.Context, countQuery string) (uint64, uint64, error) {
	query := b.Client.Query(countQuery)

	var rowCount struct {
		TotalRows     int64
		CompletedRows int64
	}

	it, err := query.Read(ctx)
	if err != nil {
		return 0, 0, err
	}

	err = it.Next(&rowCount)
	if err != nil {
		return 0, 0, err
	}

	return uint64(rowCount.TotalRows), uint64(rowCount.CompletedRows), nil
}

func findField(schema bigquery.Schema, name string) *bigquery.FieldSchema {
	for _, field := range schema {
		if field.Name == name {
			return field
		}
	}
	return nil
}

func (b *BigQueryLoader) GetMetaData(ctx context.Context, di DataInstance) (bigquery.TableMetadata, error) {

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
		logwithctx(ctx).Warnf("Expiration days out of range: %d", di.DataFile.ExpirationDays)
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
		case DataTypeBool:
			bqFieldType = bigquery.BooleanFieldType
		default:
			logrus.Warnf("Unknown field type detected: %s", v)
			continue
		}
		schema = append(schema, &bigquery.FieldSchema{Name: k, Type: bqFieldType, Required: false})
	}

	return schema
}

func (b *BigQueryLoader) LoadComplexDataItems(ctx context.Context, dataInstance DataInstance) ([]interface{}, error) {

	// JobRunName and CreationTime are required
	if len(dataInstance.JobRunName) == 0 {
		return nil, fmt.Errorf("missing Job run name")
	}

	if dataInstance.CreationTime.IsZero() {
		return nil, fmt.Errorf("missing creation time")
	}

	if dataInstance.CreationTime.Before(time.Now().Add(-12 * time.Hour)) {
		logwithctx(ctx).Warnf("Detected event processing lag with event creation time of %s", dataInstance.CreationTime.Format(time.RFC3339))
	}

	metaData := bigquery.TableMetadata{Name: dataInstance.DataFile.TableName}
	err := b.retryableTableCache(ctx, &metaData)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize table cache")
	}

	// https://cloud.google.com/bigquery/docs/sessions-intro
	// https://cloud.google.com/bigquery/docs/transactions
	// could look at using sessions and transactions
	// for now just use a 3 part sequence to capture
	// we attempted to write the data
	// the data is written
	// we succeeded in writing the data

	var inserter *bigquery.Inserter
	if !b.DryRun {
		tableCache, ok := b.tableCache[dataInstance.DataFile.TableName]

		if !ok || tableCache.table == nil {
			return nil, fmt.Errorf("invalid table reference for %s", dataInstance.DataFile.TableName)
		}
		inserter = tableCache.table.Inserter()

		// create an entry in the write log table
		q := b.Client.Query(fmt.Sprintf("INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES('%s', '%s', '%s', '%s', %s)", b.DataSetID, internalWriteLog, JobRunNameField, SourceNameField, DataPartitioningField, modifiedTime, internalWriteLogCompleted, dataInstance.JobRunName, dataInstance.Source, dataInstance.CreationTime.Format(time.RFC3339), time.Now().Format(time.RFC3339), "false"))
		runQueryWithBackoff(ctx, q, "create write log entry")
	}

	diRows := make([]interface{}, 0)

	chunkSize := dataInstance.DataFile.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 999
	}
	for _, r := range dataInstance.DataFile.ComplexRows {

		diRows = append(diRows, r)

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

	// mark our write log entry complete
	// we won't return errors here but will log warnings
	q := b.Client.Query(fmt.Sprintf("INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES('%s', '%s', '%s', '%s', %s)", b.DataSetID, internalWriteLog, JobRunNameField, SourceNameField, DataPartitioningField, modifiedTime, internalWriteLogCompleted, dataInstance.JobRunName, dataInstance.Source, dataInstance.CreationTime.Format(time.RFC3339), time.Now().Format(time.RFC3339), "true"))
	runQueryWithBackoff(ctx, q, "update write log completion")
	return nil, nil
}

func (b *BigQueryLoader) LoadDataItems(ctx context.Context, dataInstance DataInstance) ([]interface{}, error) {

	// JobRunName and CreationTime are required
	if len(dataInstance.JobRunName) == 0 {
		return nil, fmt.Errorf("missing Job run name")
	}

	if dataInstance.CreationTime.IsZero() {
		return nil, fmt.Errorf("missing creation time")
	}

	if dataInstance.CreationTime.Before(time.Now().Add(-12 * time.Hour)) {
		logwithctx(ctx).Warnf("Detected event processing lag with event creation time of %s", dataInstance.CreationTime.Format(time.RFC3339))
	}

	// https://cloud.google.com/bigquery/docs/sessions-intro
	// https://cloud.google.com/bigquery/docs/transactions
	// could look at using sessions and transactions
	// for now just use a 3 part sequence to capture
	// we attempted to write the data
	// the data is written
	// we succeeded in writing the data

	var inserter *bigquery.Inserter
	if !b.DryRun {
		tableCache, ok := b.tableCache[dataInstance.DataFile.TableName]

		if !ok || tableCache.table == nil {
			return nil, fmt.Errorf("invalid table reference for %s", dataInstance.DataFile.TableName)
		}
		inserter = tableCache.table.Inserter()

		// create an entry in the write log table
		q := b.Client.Query(fmt.Sprintf("INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES('%s', '%s', '%s', '%s', %s)", b.DataSetID, internalWriteLog, JobRunNameField, SourceNameField, DataPartitioningField, modifiedTime, internalWriteLogCompleted, dataInstance.JobRunName, dataInstance.Source, dataInstance.CreationTime.Format(time.RFC3339), time.Now().Format(time.RFC3339), "false"))
		runQueryWithBackoff(ctx, q, "create write log entry")
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

	// mark our write log entry complete
	// we won't return errors here but will log warnings
	q := b.Client.Query(fmt.Sprintf("INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES('%s', '%s', '%s', '%s', %s)", b.DataSetID, internalWriteLog, JobRunNameField, SourceNameField, DataPartitioningField, modifiedTime, internalWriteLogCompleted, dataInstance.JobRunName, dataInstance.Source, dataInstance.CreationTime.Format(time.RFC3339), time.Now().Format(time.RFC3339), "true"))
	runQueryWithBackoff(ctx, q, "update write log completion")
	return nil, nil
}

func runQueryWithBackoff(ctx context.Context, q *bigquery.Query, action string) error {
	retries := 0
	for {
		logwithctx(ctx).Infof("Attempting query for %s, retries: %d", action, retries)
		evaluateRetry := false
		job, err := q.Run(ctx)
		if err != nil {
			if !strings.Contains("jobRateLimitExceeded", err.Error()) {
				evaluateRetry = true
			} else {
				logwithctx(ctx).Warnf(fmt.Sprintf("error running %s:  %v", action, err))
				return err
			}
		}

		if job != nil {
			status, err := job.Wait(ctx)
			if err != nil {
				if !strings.Contains("jobRateLimitExceeded", err.Error()) {
					evaluateRetry = true
				} else {
					logwithctx(ctx).Warnf(fmt.Sprintf("error waiting for %s: %v", action, err))
					return err
				}
			} else if !status.Done() {
				logwithctx(ctx).Warnf(fmt.Sprintf("error status not done for %s", action))
			}
		}

		if !evaluateRetry || retries >= maxRetries {
			if evaluateRetry {
				logwithctx(ctx).Warnf(fmt.Sprintf("error exhausted retries %s: %v", action, err))
			}

			return err
		}

		retries++
		time.Sleep(time.Millisecond * time.Duration(retries) * 500)
	}
}

func insertRows(ctx context.Context, inserter *bigquery.Inserter, items []interface{}) {
	// add in backoff / retry handling
	err := inserter.Put(ctx, items)

	// do something more with the error?
	if err != nil {
		logwithctx(ctx).Errorf("Error inserting records: %v", err)
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
