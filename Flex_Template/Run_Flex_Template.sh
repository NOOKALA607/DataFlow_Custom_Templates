gcloud dataflow flex-template run "getting-started-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location "gs://learn_gcs/df_employee/templates/Flext_Template.json" \
  --parameters input_csv_file="gs://learn_gcs/df_employee/Employee_Login_Raw_Data/employee_20-7-2025.csv",output_table="learn-436612:Test.employee_login_summary" \
  --region "us-central1" \
  --additional-user-labels purpose=employee-load
