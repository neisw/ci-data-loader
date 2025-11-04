build:
	go build .
.PHONY: build

deploy-service-account:
	gcloud iam service-accounts create job-run-big-query-writer \
		--display-name job-run-big-query-writer \
		--description 'Allows ci-data-loader to read elements in the origin-ci-bucket and write data to the specified project' \
		--project openshift-gce-devel
	gsutil -m iam ch \
		serviceAccount:job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com:objectViewer \
		gs://origin-ci-test
.PHONY: deploy-service-account


deploy: build
	gcloud functions deploy LoadJobRunData \
        --gen2 \
        --region us-east1 \
        --runtime go122 \
        --source . \
        --entry-point LoadJobRunDataCloudEvent \
		--project openshift-gce-devel --retry \
		--service-account job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com \
		--memory 2048MB --timeout=300s --max-instances=100 \
		--trigger-event-filters='type=google.cloud.storage.object.v1.finalized' \
        --trigger-event-filters='bucket=test-platform-results' \
		--set-env-vars PROJECT_ID=openshift-ci-data-analysis,DATASET_ID=ci_data_autodl,PR_DATA_FILES=risk-analysis-:retry-statistics
.PHONY: deploy

delete:
	gcloud functions delete LoadJobRunData \
		--project openshift-gce-devel \
		--region us-east1
.PHONY: delete

deploy-test: build
	gcloud functions deploy LoadJobRunDataTest \
        --gen2 \
        --region us-east1 \
        --runtime go122 \
        --source . \
        --entry-point LoadJobRunDataCloudEvent \
		--project openshift-gce-devel \
		--service-account job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com \
		--memory 2048MB --timeout=300s --max-instances=10 \
		--trigger-event-filters='type=google.cloud.storage.object.v1.finalized' \
        --trigger-event-filters='bucket=test-platform-results' \
		--set-env-vars PROJECT_ID=openshift-ci-data-analysis,DATASET_ID=ci_data_autodl_test,PR_DATA_FILES=retry-statistics,MATCH_DATA_FILES=retry-statistics

.PHONY: deploy-test

delete-test:
	gcloud functions delete LoadJobRunDataTest \
		--project openshift-gce-devel \
		--region us-east1
.PHONY: delete-test
