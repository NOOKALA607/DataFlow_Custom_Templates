There are two types of Templates two run Custom Dataflow Templates
1. Classic Template
2. Flex Template

**1. Classic Template**
✅ Overview:
1. The original template format in Dataflow. 
2. Stores pipeline job graph as a JSON file in Cloud Storage.
3. Created using the --template_location flag.

✅ Characteristics:
1. Immutable: You cannot change the pipeline logic once the template is created.
2. Supports limited parameterization.
3. Does not support staging Docker images or dependencies directly.

Usage Example:
**python your_pipeline.py \
  --runner DataflowRunner \
  --template_location gs://your-bucket/templates/template_name.json \
  --project your-project \
  --region your-region**
  

** 2. Flex Template**
✅ Overview:
1. A more flexible and modern alternative to classic templates.
2. Uses a Docker container to run your pipeline.
3. Allows more complex logic, dependency management, and dynamic parameters.

✅ Characteristics:
1. Fully customizable and version-controlled via Docker images.
2. Template is a JSON metadata file pointing to a Docker image and pipeline options.
3. Supports multi-language SDKs (Java, Python).
4. Useful for production-grade pipelines and CI/CD.

**Usage Example:**
✅ Usage (Creation):
**gcloud dataflow flex-template build \
  gs://your-bucket/templates/template_metadata.json \
  --image-gcr-path gcr.io/your-project/your-image \
  --sdk-language "PYTHON" \
  --metadata-file metadata.json**
  
✅ Usage (Execution):
**gcloud dataflow flex-template run "job-name" \
  --template-file-gcs-location gs://your-bucket/templates/template_metadata.json \
  --parameters inputFile=gs://your/input.csv,outputTable=project:dataset.table**

   
Dataflow Build and Run Flex Template Google GCP Document - https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates
Create Classic Template Google GCP Document - https://cloud.google.com/dataflow/docs/guides/templates/creating-templates
Run Classic Template Google GCP Document - https://cloud.google.com/dataflow/docs/guides/templates/running-templates
