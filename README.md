# ci-data-loader
The cloud functions in this repository are used to load known prow job result artifacts
to the specified big query project.

The functions operate on origin-ci-test and must therefore be deployed in the openshift-gce-devel
project. The service account job-run-big-query-writer@openshift-gce-devel.iam.gserviceaccount.com
was created ahead of time and given storage viewer access on the origin-ci-test bucket. The BigQuery
project specified in the deployment env-vars will need to add the service account and 
allow it read / write / create access.  

During first deployment
the function should *not* be accessible to external viewers.
