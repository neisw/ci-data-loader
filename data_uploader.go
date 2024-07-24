package cidataloader

import (
	"context"
	"fmt"
	"time"
)

type DataLoader interface {
	ValidateTable(ctx context.Context, dataInstance DataInstance) error
	FindExistingData(ctx context.Context, partitionTime time.Time, partitionColumn, tableName, jobRunName, source string) (bool, error)
	LoadDataItems(ctx context.Context, dataInstance DataInstance) ([]interface{}, error)
}

type DataUploader struct {
	dataLoader   DataLoader
	dataInstance DataInstance
}

type SimpleUploader interface {
	upload(context context.Context) error
}

func generateDataUploader(dataInstance DataInstance, dataLoader DataLoader) (SimpleUploader, error) {
	if dataInstance.DataFile == nil {
		return nil, fmt.Errorf("invalid data Instance")
	}

	loader := DataUploader{
		dataLoader:   dataLoader,
		dataInstance: dataInstance,
	}
	return &loader, nil
}

func (d *DataUploader) upload(ctx context.Context) error {
	// if the table exists validate the metadata matches or update it
	err := d.dataLoader.ValidateTable(ctx, d.dataInstance)
	if err != nil {
		return err
	}

	existing, err := d.dataLoader.FindExistingData(ctx, d.dataInstance.CreationTime, d.dataInstance.DataFile.PartitionColumn, d.dataInstance.DataFile.TableName, d.dataInstance.JobRunName, d.dataInstance.Source)

	if err != nil {
		return err
	} else if existing {
		// we don't want duplicate data so if we have records already then log a warning and return nil so the function doesn't retry
		logwithctx(ctx).Warnf("found existing data for %s/%s", d.dataInstance.JobRunName, d.dataInstance.Source)
		return nil
	}

	_, err = d.dataLoader.LoadDataItems(ctx, d.dataInstance)
	if err != nil {
		return err
	}

	return nil
}
