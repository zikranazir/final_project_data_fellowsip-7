# List of Command Execution

1. Create GCP service account and generate a JSON key
2. Export a JSON key to Google Application Credential variable
```
export GOOGLE_APPLICATION_CREDENTIALS=~/<your-folder>/<your-service-account>.json
``` 
3. Refresh service-account's auth-token through gcloud CLI
```
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
```
Alternatively, you can authenticate using OAuth like
```
gcloud auth application-default login
```
4. Initialize state file (.tfstate)
```
terraform init
```
5. Check the new infra plan
```
terraform plan -var="project=<your-gcp-project-id>"
```
6. Apply the new infra plan
```
terraform apply -var="project=<your-gcp-project-id>"
```
Tadaa...! Your infra has been automatically created.
7. Delete infra after your work done, to avoid costs on any running services
```
terraform destroy -var="project=<your-gcp-project-id>"
```