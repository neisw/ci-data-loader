build:
	go build .
.PHONY: build

deploy: build
	gcloud functions deploy LoadJobRunData \
		--project openshift-gce-devel --runtime go120 \
		--service-account job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com \
		--memory 128MB --timeout=300s --max-instances=10 \
		--trigger-resource origin-ci-test --trigger-event google.storage.object.finalize \
		--set-env-vars PROJECT_ID=openshift-ci-data-analysis,DATASET_ID=fsbabcock_test
.PHONY: deploy

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
