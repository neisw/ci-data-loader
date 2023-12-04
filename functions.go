package cidataloader

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"os"
	"path"
	"strings"
	"time"
)

type GCSEvent struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	// SelfLink                string                 `json:"selfLink"`
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
	// Generation              string                 `json:"generation"`
	// Metageneration          string                 `json:"metageneration"`
	ContentType string    `json:"contentType"`
	TimeCreated time.Time `json:"timeCreated"`
	Updated     time.Time `json:"updated"`
	// TemporaryHold           bool                   `json:"temporaryHold"`
	// EventBasedHold          bool                   `json:"eventBasedHold"`
	// RetentionExpirationTime time.Time              `json:"retentionExpirationTime"`
	// StorageClass            string                 `json:"storageClass"`
	// TimeStorageClassUpdated time.Time              `json:"timeStorageClassUpdated"`
	Size      string `json:"size"`
	MD5Hash   string `json:"md5Hash"`
	MediaLink string `json:"mediaLink"`
	// ContentEncoding         string                 `json:"contentEncoding"`
	// ContentDisposition      string                 `json:"contentDisposition"`
	// CacheControl            string                 `json:"cacheControl"`
	Metadata map[string]interface{} `json:"metadata"`
	// CRC32C                  string                 `json:"crc32c"`
	// ComponentCount          int                    `json:"componentCount"`
	// Etag                    string                 `json:"etag"`
	// CustomerEncryption      struct {
	// 	EncryptionAlgorithm string `json:"encryptionAlgorithm"`
	// 	KeySha256           string `json:"keySha256"`
	// }
	// KMSKeyName    string `json:"kmsKeyName"`
	// ResourceState string `json:"resourceState"`
}

const (
	AutoDataLoaderSuffix  = "autodl.json"
	DataSetEnv            = "DATASET_ID"
	ProjectIdEnv          = "PROJECT_ID"
	PRJobsEnabledEnv      = "PR_JOBS_ENABLED"      // local testing only
	GCSCredentialsFileEnv = "GCS_CREDENTIALS_FILE" // local testing only
)

var bigQueryClient *bigquery.Client
var storageClient *storage.Client
var bigQueryLoader BigQueryLoader

func init() {
	err := initGlobals(context.TODO())

	if err != nil {
		logrus.Errorf("Error initializing globals: %v", err)
	}
}

func initGlobals(ctx context.Context) error {
	var err error
	projectID := os.Getenv(ProjectIdEnv)
	if len(projectID) == 0 {
		return fmt.Errorf("Missing ENV Variable: %s", ProjectIdEnv)
	}

	dataSetId := os.Getenv(DataSetEnv)
	if len(dataSetId) == 0 {
		return fmt.Errorf("Missing ENV Variable: %s", DataSetEnv)
	}

	bigQueryLoader = BigQueryLoader{ProjectID: projectID, DataSetID: dataSetId}
	credentialsPath := os.Getenv(GCSCredentialsFileEnv)
	if len(credentialsPath) > 0 {
		bigQueryClient, err = bigquery.NewClient(ctx,
			bigQueryLoader.ProjectID,
			option.WithCredentialsFile(credentialsPath),
		)
	} else {
		bigQueryClient, err = bigquery.NewClient(ctx,
			bigQueryLoader.ProjectID,
		)
	}

	if err != nil {
		logrus.Errorf("Failed to initialize new bigquery client: %v", err)
		return err
	}

	// Technically we will leak connections since we
	// initialize these globally and don't know when our CF will close
	// but this is a trade-off for the 'warm start' and shouldn't be an issue
	// defer bigQueryClient.Close()

	bigQueryLoader.Client = bigQueryClient

	if len(credentialsPath) > 0 {
		storageClient, err = storage.NewClient(context.TODO(), option.WithScopes(storage.ScopeReadOnly), option.WithCredentialsFile(credentialsPath))
	} else {
		storageClient, err = storage.NewClient(context.TODO(), option.WithScopes(storage.ScopeReadOnly))
	}
	if err != nil {
		logrus.Errorf("Failed to initialize new storage client: %v", err)
		return err
	}

	// Technically we will leak connections since we
	// initialize these globally and don't know when our CF will close
	// but this is a trade-off for the 'warm start' and shouldn't be an issue
	// defer storageClient.Close()

	return nil
}

func LoadJobRunData(ctx context.Context, e GCSEvent) error {

	var simpleUploader SimpleUploader
	var err error

	jobRunData, err := generateJobRunDataEvent(&e)

	if err != nil {
		return err
	}

	prJobsEnabled := false
	prJobsEnabledFlag := os.Getenv(PRJobsEnabledEnv)
	if len(prJobsEnabledFlag) > 0 && prJobsEnabledFlag == "Y" {
		prJobsEnabled = true
	}

	err = jobRunData.parseJob(prJobsEnabled)
	if err != nil {
		return err
	}

	if len(jobRunData.BuildID) == 0 || len(jobRunData.Job) == 0 || len(jobRunData.Filename) == 0 {
		logrus.Debugf("Skipping event for: %v", e)
	}

	switch {

	case "job_metrics.json" == jobRunData.Filename:
		simpleUploader, err = generateMetricsUploader(storageClient, ctx, jobRunData, &bigQueryLoader)

	case strings.HasSuffix(jobRunData.Filename, AutoDataLoaderSuffix):
		simpleUploader, err = generateDataFileUploader(storageClient, ctx, jobRunData, &bigQueryLoader)

	case strings.HasPrefix(jobRunData.Filename, "e2e-events"):
		simpleUploader, err = generateIntervalUploader(storageClient, ctx, jobRunData, &bigQueryLoader)

	default:
		return nil
	}

	if err != nil {
		logrus.Errorf("Failed to initialize simple loader: %v", err)
		return err
	}

	err = simpleUploader.upload(ctx)

	if err != nil {
		logrus.Errorf("Failed to upload loader: %v", err)
		return err
	}

	return nil
}

type JobRunDataEvent struct {
	Job      string
	BuildID  string
	Filename string
	GCSEvent *GCSEvent
}

func generateJobRunDataEvent(event *GCSEvent) (*JobRunDataEvent, error) {
	if event == nil {
		return nil, fmt.Errorf("missing gcs event")
	}

	return &JobRunDataEvent{GCSEvent: event}, nil
}

func (j *JobRunDataEvent) parseJob(prJobsEnabled bool) error {
	if j.GCSEvent == nil {
		return fmt.Errorf("invalid GCSEvent")
	}

	parts := strings.Split(j.GCSEvent.Name, "/")
	if len(parts) < 4 {
		return nil
	}

	var build, job, base string
	switch {
	case parts[0] == "logs":

		job = parts[1]
		build = parts[2]
		base = path.Base(j.GCSEvent.Name)

		switch {
		case strings.HasPrefix(job, "periodic-ci-openshift-release-"),
			strings.HasPrefix(job, "release-openshift-"):
		default:
			// log.Printf("Skip job that is not a release job: %s", e.Name)
			return nil
		}
	case parts[0] == "pr-logs":
		if !prJobsEnabled {
			return nil
		}
		// pr-logs/pull/28431/pull-ci-openshift-origin-master-e2e-gcp-ovn-upgrade/1730318696951320576
		job = parts[3]
		build = parts[4]
		base = path.Base(j.GCSEvent.Name)
	default:
		// log.Printf("Skip job that is not postsubmit/periodic: %s", e.Name)
		return nil
	}

	j.Filename = base
	j.Job = job
	j.BuildID = build

	return nil

}
