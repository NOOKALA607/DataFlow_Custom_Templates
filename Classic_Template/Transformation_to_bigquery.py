import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import csv
from io import StringIO

class ParseCSVRow(beam.DoFn):
    def process(self, element):
        import csv
        from io import StringIO
        if element.startswith("EmployeeID"):
            return []

        reader = csv.reader(StringIO(element))
        for row in reader:
            if len(row) < 5:
                return []

            try:
                employee_id = row[0]
                name = row[1]
                department = row[2]
                login_time = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                logout_time = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")

                return [(employee_id, {
                    'name': name,
                    'department': department,
                    'login_time': login_time,
                    'logout_time': logout_time
                })]
            except Exception as e:
                return []

class ComputeFirstLoginLastLogout(beam.DoFn):
    def process(self, element):
        employee_id, records = element
        name = records[0]['name']
        department = records[0]['department']

        login_times = [r['login_time'] for r in records]
        logout_times = [r['logout_time'] for r in records]

        return [{
            'employee_id': employee_id,
            'name': name,
            'department': department,
            'first_login': min(login_times).strftime("%Y-%m-%d %H:%M:%S"),
            'last_logout': max(logout_times).strftime("%Y-%m-%d %H:%M:%S"),
        }]

def run():
    input_path = 'gs://learn_gcs/df_employee/Employee_Login_Raw_Data/employee_20-7-2025.csv'
    output_table = 'learn-436612:Test.employee_login_summary'

    pipeline_options = PipelineOptions(
        runner='DataflowRunner',  # or 'DirectRunner' for local testing
        project='learn-436612',
        region='us-central1',
        temp_location='gs://learn_gcs/temp/',
        staging_location='gs://learn_gcs/staging/',
        job_name='employee-login-summary-job'
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | 'Parse CSV Rows' >> beam.ParDo(ParseCSVRow())
            | 'Group by Employee ID' >> beam.GroupByKey()
            | 'Compute First Login and Last Logout' >> beam.ParDo(ComputeFirstLoginLastLogout())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema='employee_id:STRING, name:STRING, department:STRING, first_login:STRING, last_logout:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
