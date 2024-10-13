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
	"regexp"
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

var clientsCache *ClientsCache
var buildIdMatch = regexp.MustCompile(`^\d`)

type ClientsCache struct {
	storageClient  *storage.Client
	bigQueryLoader *BigQueryLoader
	cachedTime     time.Time
}

func init() {
	initClientCache()
}

func initClientCache() {
	newCache, err := initGlobals(context.TODO())

	if err != nil {
		logrus.Errorf("Error initializing globals: %v", err)
		return
	}

	// if we aren't nil then attempt to close any open connections
	if clientsCache != nil {
		clientsCache.storageClient.Close()
		clientsCache.bigQueryLoader.Client.Close()
	}

	clientsCache = newCache
}

func initGlobals(ctx context.Context) (*ClientsCache, error) {
	var err error
	projectID := os.Getenv(ProjectIdEnv)
	if len(projectID) == 0 {
		return nil, fmt.Errorf("missing ENV Variable: %s", ProjectIdEnv)
	}

	dataSetId := os.Getenv(DataSetEnv)
	if len(dataSetId) == 0 {
		return nil, fmt.Errorf("missing ENV Variable: %s", DataSetEnv)
	}

	var bigQueryClient *bigquery.Client
	newCache := ClientsCache{cachedTime: time.Now()}
	newCache.bigQueryLoader = &BigQueryLoader{ProjectID: projectID, DataSetID: dataSetId}
	credentialsPath := os.Getenv(GCSCredentialsFileEnv)
	if len(credentialsPath) > 0 {
		bigQueryClient, err = bigquery.NewClient(ctx,
			newCache.bigQueryLoader.ProjectID,
			option.WithCredentialsFile(credentialsPath),
		)
	} else {
		bigQueryClient, err = bigquery.NewClient(ctx,
			newCache.bigQueryLoader.ProjectID,
		)
	}

	if err != nil {
		logrus.Errorf("Failed to initialize new bigquery client: %v", err)
		return nil, err
	}

	// Technically we will leak connections since we
	// initialize these globally and don't know when our CF will close
	// but this is a trade-off for the 'warm start' and shouldn't be an issue
	// defer bigQueryClient.Close()

	newCache.bigQueryLoader.Client = bigQueryClient
	err = newCache.bigQueryLoader.Init(ctx)

	if err != nil {
		logrus.Errorf("Failed to initialize new bigquery loader: %v", err)
		return nil, err
	}

	if len(credentialsPath) > 0 {
		newCache.storageClient, err = storage.NewClient(context.TODO(), option.WithScopes(storage.ScopeReadOnly), option.WithCredentialsFile(credentialsPath))
	} else {
		newCache.storageClient, err = storage.NewClient(context.TODO(), option.WithScopes(storage.ScopeReadOnly))
	}
	if err != nil {
		logrus.Errorf("Failed to initialize new storage client: %v", err)
		return nil, err
	}

	// Technically we will leak connections since we
	// initialize these globally and don't know when our CF will close
	// but this is a trade-off for the 'warm start' and shouldn't be an issue
	// defer storageClient.Close()

	return &newCache, nil
}

func LoadJobRunDataTest(ctx context.Context, e GCSEvent) error {
	return LoadJobRunData(ctx, e)
}

func LoadJobRunData(ctx context.Context, e GCSEvent) error {

	var simpleUploader SimpleUploader
	var err error

	// initially added when our SA was deleted
	// may not be needed but if we are long running then
	// periodically refresh our clients
	if clientsCache == nil || clientsCache.cachedTime.Before(time.Now().Add(-24*time.Hour)) {
		initClientCache()
	}

	jobRunData, err := generateJobRunDataEvent(&e)

	if err != nil {
		logrus.Errorf("Returning generateJobRunDataEvent error for %v", e)
		return err
	}

	prJobsEnabled := false
	prJobsEnabledFlag := os.Getenv(PRJobsEnabledEnv)
	if len(prJobsEnabledFlag) > 0 && prJobsEnabledFlag == "Y" {
		prJobsEnabled = true
	}

	err = jobRunData.parseJob(prJobsEnabled)
	if err != nil {
		logrus.Errorf("Returning parseJob error for %v", e)
		return err
	}

	if len(jobRunData.BuildID) == 0 || len(jobRunData.Job) == 0 || len(jobRunData.Filename) == 0 {
		logrus.Debugf("Skipping event for: %v", e)
	}

	ctx = addlogctx(ctx, jobRunData.BuildID, jobRunData.Job, jobRunData.Filename)

	// might move this into the interface
	var dataType = ""

	switch {

	case "job_metrics.json" == jobRunData.Filename:
		simpleUploader, err = generateMetricsUploader(clientsCache.storageClient, ctx, jobRunData, clientsCache.bigQueryLoader)
		dataType = "metrics"

	case strings.HasSuffix(jobRunData.Filename, AutoDataLoaderSuffix):
		simpleUploader, err = generateDataFileUploader(clientsCache.storageClient, ctx, jobRunData, clientsCache.bigQueryLoader)
		dataType = "autodl"

	case strings.HasPrefix(jobRunData.Filename, "e2e-events") && strings.HasSuffix(jobRunData.Filename, ".json"):
		simpleUploader, err = generateIntervalUploader(clientsCache.storageClient, ctx, jobRunData, clientsCache.bigQueryLoader)
		dataType = "intervals"

	default:
		return nil
	}

	// no point returning error if we can't process it
	// see cases where a bad file is continually reprocessed
	if err != nil {
		logwithctx(ctx).Errorf("Failed to initialize simple loader: %v - %v", err, e)
		return nil
	}

	startTime := time.Now()
	err = simpleUploader.upload(ctx)

	if err != nil {
		logwithctx(ctx).Errorf("Failed to upload loader: %v", err)
		return err
	}

	diff := int64(time.Now().Sub(startTime) / time.Millisecond)
	logwithctx(ctx).Infof("processing %s upload completed: %dms", dataType, diff)
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
		// we want to collect risk-analysis artifacts for pr jobs
		fileNameBase := path.Base(j.GCSEvent.Name)
		if !prJobsEnabled && !strings.HasPrefix(fileNameBase, "risk-analysis-") {
			return nil
		}
		// pr-logs/pull/28431/pull-ci-openshift-origin-master-e2e-gcp-ovn-upgrade/1730318696951320576
		baseIndex := 3
		padding := 0
		// try to detect if the 4 index is numeric, if not bump it out 1
		if !buildIdMatch.MatchString(parts[baseIndex+padding+1]) {
			padding += 1
		}

		job = parts[baseIndex+padding]
		build = parts[baseIndex+padding+1]
		base = fileNameBase
		logrus.Infof("pr-logs job for %s: Job: %s, Build: %s, Base: %s", j.GCSEvent.Name, job, build, base)
	default:
		// log.Printf("Skip job that is not postsubmit/periodic: %s", e.Name)
		return nil
	}

	j.Filename = base
	j.Job = job
	j.BuildID = build

	return nil

}
