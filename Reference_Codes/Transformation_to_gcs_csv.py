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
        return [
            acc['EmployeeID'],
            acc['Name'],
            acc['Department'],
            acc['EarliestLogin'].strftime("%Y-%m-%d %H:%M:%S") if acc['EarliestLogin'] else '',
            acc['LatestLogout'].strftime("%Y-%m-%d %H:%M:%S") if acc['LatestLogout'] else ''
        ]

def format_as_csv(row):
    return ','.join(row)

def run():
    input_file = r'F:\GCP_Projects\DataFlow_Custom_Templates\employee_login_raw_data\employee_20-7-2025.csv'
    output_file = r'F:\GCP_Projects\DataFlow_Custom_Templates\employee_login_processed_data\employee_20-7-2025.csv'

    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        header = ['EmployeeID', 'Name', 'Department', 'FirstLoginTime', 'LastLogoutTime']

        data_rows = (
            p
            | 'Read Input File' >> beam.io.ReadFromText(input_file)
            | 'Parse CSV Rows' >> beam.ParDo(ParseCSVRow())
            | 'Group By EmployeeID' >> beam.CombinePerKey(CombineEmployeeTimes())
            | 'Drop Key' >> beam.Values()
            | 'Format as CSV Row' >> beam.Map(format_as_csv)
        )

        # Add header as a separate PCollection
        header_row = (
            p
            | 'Create Header' >> beam.Create([f"{','.join(header)}"])
        )

        # Merge header and data and write output
        (
            (header_row, data_rows)
            | 'Flatten Header + Data' >> beam.Flatten()
            | 'Write Output CSV' >> beam.io.WriteToText(
                output_file.replace('.csv', ''),
                file_name_suffix='.csv',
                shard_name_template=''  # Avoid file sharding (single file output)
            )
        )

if __name__ == '__main__':
    run()
