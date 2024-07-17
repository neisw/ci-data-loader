build:
	go build .
.PHONY: build

deploy: build
	gcloud functions deploy LoadJobRunData \
		--project openshift-gce-devel --retry --runtime go121 \
		--service-account job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com \
		--memory 2048MB --timeout=300s --max-instances=20 \
		--trigger-resource test-platform-results --trigger-event google.storage.object.finalize \
		--set-env-vars PROJECT_ID=openshift-ci-data-analysis,DATASET_ID=ci_data_autodl \
		--docker-registry=artifact-registry
.PHONY: deploy

deploy-test: build
	gcloud functions deploy LoadJobRunDataTest \
		--project openshift-gce-devel --runtime go121 \
		--service-account job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com \
		--memory 2048MB --timeout=300s --max-instances=10 \
		--trigger-resource test-platform-results --trigger-event google.storage.object.finalize \
		--set-env-vars PROJECT_ID=openshift-ci-data-analysis,DATASET_ID=ci_data_autodl_test \
		--docker-registry=artifact-registry
.PHONY: deploy-test

deploy-service-account:
	gcloud iam service-accounts create job-run-big-query-writer \
		--display-name job-run-big-query-writer \
		--description 'Allows ci-data-loader to read elements in the origin-ci-bucket and write data to the specified project' \
		--project openshift-gce-devel
	gsutil -m iam ch \
		serviceAccount:job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com:objectViewer \
		gs://origin-ci-test
.PHONY: deploy-service-account

delete:
	gcloud functions delete LoadJobRunData \
		--project openshift-gce-devel
.PHONY: delete

delete-test:
	gcloud functions delete LoadJobRunDataTest \
		--project openshift-gce-devel
.PHONY: delete-test
