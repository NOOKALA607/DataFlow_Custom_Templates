import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import csv
from io import StringIO

class ParseCSVRow(beam.DoFn):
    def process(self, element):
        if element.startswith("EmployeeID"):
            return []

        reader = csv.reader(StringIO(element))
        for row in reader:
            if len(row) < 5:
                return []
            employee_id, name, dept, login_str, logout_str = row
            try:
                login_time = datetime.strptime(login_str.strip(), "%Y-%m-%d %H:%M:%S")
                logout_time = datetime.strptime(logout_str.strip(), "%Y-%m-%d %H:%M:%S") if logout_str.strip() else None
                yield (employee_id, {
                    'EmployeeID': employee_id,
                    'Name': name,
                    'Department': dept,
                    'LoginTime': login_time,
                    'LogoutTime': logout_time
                })
            except:
                return []

class CombineEmployeeTimes(beam.CombineFn):
    def create_accumulator(self):
        return {
            'EmployeeID': None,
            'Name': None,
            'Department': None,
            'EarliestLogin': None,
            'LatestLogout': None
        }

    def add_input(self, acc, input):
        acc['EmployeeID'] = input['EmployeeID']
        acc['Name'] = input['Name']
        acc['Department'] = input['Department']
        login_time = input['LoginTime']
        logout_time = input['LogoutTime']

        if acc['EarliestLogin'] is None or login_time < acc['EarliestLogin']:
            acc['EarliestLogin'] = login_time

        if logout_time:
            if acc['LatestLogout'] is None or logout_time > acc['LatestLogout']:
                acc['LatestLogout'] = logout_time

        return acc

    def merge_accumulators(self, accumulators):
        result = self.create_accumulator()
        for acc in accumulators:
            if result['EmployeeID'] is None:
                result['EmployeeID'] = acc['EmployeeID']
                result['Name'] = acc['Name']
                result['Department'] = acc['Department']

            if acc['EarliestLogin'] and (result['EarliestLogin'] is None or acc['EarliestLogin'] < result['EarliestLogin']):
                result['EarliestLogin'] = acc['EarliestLogin']
            if acc['LatestLogout'] and (result['LatestLogout'] is None or acc['LatestLogout'] > result['LatestLogout']):
                result['LatestLogout'] = acc['LatestLogout']
        return result

    def extract_output(self, acc):
        return {
            'EmployeeID': acc['EmployeeID'],
            'Name': acc['Name'],
            'Department': acc['Department'],
            'FirstLoginTime': acc['EarliestLogin'].strftime("%Y-%m-%d %H:%M:%S") if acc['EarliestLogin'] else '',
            'LastLogoutTime': acc['LatestLogout'].strftime("%Y-%m-%d %H:%M:%S") if acc['LatestLogout'] else ''
        }

def run():
    input_file = 'gs://learn_gcs/df_employee/Employee_Login_Raw_Data/employee_20-7-2025.csv'  # Update this path
    project_id = 'learn-436612'         # Update this
    dataset = 'Employee_Data'                  # Update this
    table = 'Employee_Login_Summary'           # Update this

    pipeline_options = PipelineOptions(
        project=project_id,
        temp_location='gs://learn_gcs/temp',  # Update this
        region='us-central1',
        runner='DataflowRunner'  # Use DirectRunner for local runs
    )

    bq_schema = {
        'fields': [
            {'name': 'EmployeeID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Department', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'FirstLoginTime', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastLogoutTime', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read Input File' >> beam.io.ReadFromText(input_file)
            | 'Parse CSV Rows' >> beam.ParDo(ParseCSVRow())
            | 'Group By EmployeeID' >> beam.CombinePerKey(CombineEmployeeTimes())
            | 'Drop Key' >> beam.Values()
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table=table,
                dataset=dataset,
                project=project_id,
                schema=bq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
