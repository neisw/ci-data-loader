package cidataloader

import (
	"context"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	_ "github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// StorageObjectData contains metadata about a Cloud Storage object.
type StorageObjectData struct {
	Bucket      string            `json:"bucket"`
	Name        string            `json:"name"`
	Size        int64             `json:"size,string"`
	ContentType string            `json:"contentType"`
	TimeCreated time.Time         `json:"timeCreated"`
	Updated     time.Time         `json:"updated"`
	MD5Hash     string            `json:"md5Hash"`
	Metadata    map[string]string `json:"metadata"`
}

// GCSEvent is kept for backward compatibility with existing code
type GCSEvent struct {
	Kind        string                 `json:"kind"`
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Bucket      string                 `json:"bucket"`
	ContentType string                 `json:"contentType"`
	TimeCreated time.Time              `json:"timeCreated"`
	Updated     time.Time              `json:"updated"`
	Size        string                 `json:"size"`
	MD5Hash     string                 `json:"md5Hash"`
	MediaLink   string                 `json:"mediaLink"`
	Metadata    map[string]interface{} `json:"metadata"`
}

const (
	AutoDataLoaderSuffix  = "autodl.json"
	DataSetEnv            = "DATASET_ID"
	ProjectIdEnv          = "PROJECT_ID"
	GCSCredentialsFileEnv = "GCS_CREDENTIALS_FILE" // local testing only
	PRDataFiles           = "PR_DATA_FILES"
	MatchDataFiles        = "MATCH_DATA_FILES"
)

var clientsCache *ClientsCache
var buildIdMatch = regexp.MustCompile(`^\d`)

type ClientsCache struct {
	storageClient  *storage.Client
	bigQueryLoader *BigQueryLoader
	cachedTime     time.Time
	prJobsEnabled  bool
	prDataFiles    []string
	matchDataFiles []string
}

func init() {
	initClientCache()
	// Register the CloudEvent function for gen2
	// functions.CloudEvent("LoadJobRunData", loadJobRunDataCloudEvent)
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

	prDataFiles := os.Getenv(PRDataFiles)
	if len(prDataFiles) > 0 {
		// use : as a delimiter
		dataFiles := strings.Split(prDataFiles, ":")
		if len(dataFiles) > 0 {
			newCache.prDataFiles = dataFiles
		}
	}

	matchDataFiles := os.Getenv(MatchDataFiles)
	if len(matchDataFiles) > 0 {
		// use : as a delimiter
		dataFiles := strings.Split(matchDataFiles, ":")
		if len(dataFiles) > 0 {
			newCache.matchDataFiles = dataFiles
		}
	}

	// Technically we will leak connections since we
	// initialize these globally and don't know when our CF will close
	// but this is a trade-off for the 'warm start' and shouldn't be an issue
	// defer storageClient.Close()

	return &newCache, nil
}

// LoadJobRunDataCloudEvent is the CloudEvent handler for gen2 functions
func LoadJobRunDataCloudEvent(ctx context.Context, e event.Event) error {
	logrus.Infof("Event ID: %s", e.ID())
	logrus.Infof("Event Type: %s", e.Type())

	var data StorageObjectData
	if err := e.DataAs(&data); err != nil {
		logrus.Errorf("Failed to parse CloudEvent data: %v", err)
		return fmt.Errorf("event.DataAs: %w", err)
	}

	// Convert StorageObjectData to GCSEvent for backward compatibility
	gcsEvent := GCSEvent{
		Kind:        "storage#object",
		ID:          e.ID(),
		Name:        data.Name,
		Bucket:      data.Bucket,
		ContentType: data.ContentType,
		TimeCreated: data.TimeCreated,
		Updated:     data.Updated,
		Size:        fmt.Sprintf("%d", data.Size),
		MD5Hash:     data.MD5Hash,
		Metadata:    make(map[string]interface{}),
	}

	// Convert metadata from map[string]string to map[string]interface{}
	for k, v := range data.Metadata {
		gcsEvent.Metadata[k] = v
	}

	return LoadJobRunData(ctx, gcsEvent)
}

func LoadJobRunDataTest(ctx context.Context, e GCSEvent) error {
	return LoadJobRunData(ctx, e)
}

func LoadJobRunData(ctx context.Context, e GCSEvent) error {

	var simpleUploader SimpleUploader
	var err error
	startTime := time.Now()

	// initially added when our SA was deleted
	// may not be needed but if we are long-running then
	// periodically refresh our clients
	if clientsCache == nil || clientsCache.cachedTime.Before(time.Now().Add(-24*time.Hour)) {
		initClientCache()
	}

	jobRunData, err := generateJobRunDataEvent(&e)

	if err != nil {
		logrus.Errorf("Returning generateJobRunDataEvent error for %v", e)
		return err
	}

	err = jobRunData.parseJob(clientsCache.prDataFiles, clientsCache.matchDataFiles)
	if err != nil {
		logrus.Errorf("Returning parseJob error for %v", e)
		return err
	}

	if len(jobRunData.BuildID) == 0 || len(jobRunData.Job) == 0 || len(jobRunData.Filename) == 0 {
		logrus.Debugf("Skipping event for: %v", e)
		return nil
	}

	ctx = addlogctx(ctx, jobRunData.BuildID, jobRunData.Job, jobRunData.Filename)

	// might move this into the interface
	var dataType = ""

	switch {

	case strings.HasPrefix(jobRunData.Filename, "e2e-events") && strings.HasSuffix(jobRunData.Filename, ".json"):
		// Streaming loader won't return a SimpleUploader object since it handles the loading as it processes the data
		err = generateStreamingComplexIntervalLoader(clientsCache.storageClient, ctx, jobRunData, clientsCache.bigQueryLoader)
		dataType = "intervals"

	case "job_metrics.json" == jobRunData.Filename:
		simpleUploader, err = generateMetricsUploader(clientsCache.storageClient, ctx, jobRunData, clientsCache.bigQueryLoader)
		dataType = "metrics"

	case strings.HasSuffix(jobRunData.Filename, AutoDataLoaderSuffix):
		simpleUploader, err = generateDataFileUploader(clientsCache.storageClient, ctx, jobRunData, clientsCache.bigQueryLoader)
		dataType = "autodl"

	default:
		return nil
	}

	// no point returning error if we can't process it
	// see cases where a bad file is continually reprocessed
	if err != nil {
		logwithctx(ctx).Errorf("Failed to initialize simple loader: %v - %v", err, e)
		return nil
	}

	// intervals are handled separately so simpleUploader will be nil
	if simpleUploader != nil {
		err = simpleUploader.upload(ctx)

		if err != nil {
			logwithctx(ctx).Errorf("Failed to upload loader: %v", err)
			return err
		}
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

func (j *JobRunDataEvent) parseJob(prDataFiles, matchDataFiles []string) error {
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
		// we want to collect limited artifacts for pr jobs
		fileNameBase := path.Base(j.GCSEvent.Name)
		collectPrArtifacts := false
		for _, prefix := range prDataFiles {
			if strings.HasPrefix(fileNameBase, prefix) {
				collectPrArtifacts = true
				break
			}
		}
		if !collectPrArtifacts {
			return nil
		}
		// pr-logs/pull/28431/pull-ci-openshift-origin-master-e2e-gcp-ovn-upgrade/1730318696951320576
		baseIndex := 3
		padding := 0
		// try to detect if the 4 index is numeric, if not bump it out 1

		if len(parts) < 5 {
			logrus.Infof("Unexpected job path for parsing build id from pr-logs job:  %s", j.GCSEvent.Name)
			return nil
		}
		if !buildIdMatch.MatchString(parts[baseIndex+padding+1]) {
			if len(parts) > 5 {
				padding += 1
			} else {
				logrus.Infof("failed to parse build id for pr-logs job:  %s", j.GCSEvent.Name)
				return nil
			}

		}

		job = parts[baseIndex+padding]
		build = parts[baseIndex+padding+1]
		base = fileNameBase
		logrus.Infof("pr-logs job for %s: Job: %s, Build: %s, Base: %s", j.GCSEvent.Name, job, build, base)
	default:
		// log.Printf("Skip job that is not postsubmit/periodic: %s", e.Name)
		return nil
	}

	if len(matchDataFiles) > 0 {
		found := false
		for _, matchDataFile := range matchDataFiles {
			if strings.HasPrefix(base, matchDataFile) {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		logrus.Infof("Data file match found for job for %s: Job: %s, Build: %s, Base: %s", j.GCSEvent.Name, job, build, base)
	}

	j.Filename = base
	j.Job = job
	j.BuildID = build

	return nil

}
