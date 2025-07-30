gcloud dataflow flex-template build gs://learn_gcs/df_employee/templates/Flext_Template.json \
 --image-gcr-path "us-central1-docker.pkg.dev/learn-436612/employee-template/employee-service" \
 --sdk-language "PYTHON" \
 --flex-template-base-image "PYTHON3" \
 --metadata-file "metadata.json" \
 --py-path "." \
 --env "FLEX_TEMPLATE_PYTHON_PY_FILE=Transformation_to_bigquery.py" \
 --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"