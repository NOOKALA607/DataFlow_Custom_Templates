#!/bin/bash

python F:/GCP_Projects/DataFlow_Custom_Templates/Classic_Template/Transformation_to_bigquery.py \
--runner=DataflowRunner \
--project=learn-436612 \
--region=us-central1 \
--staging_location=gs://learn_gcs/df_employee/staging \
--temp_location=gs://learn_gcs/df_employee/temp \
--template_location=gs://learn_gcs/df_employee/templates/EMP_Login_BQ_Load_Template
