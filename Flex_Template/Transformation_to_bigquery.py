import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import ValueProvider
from datetime import datetime
import csv
from io import StringIO


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_csv_file',
            type=str,
            help='GCS path to the input CSV file'
        )
        parser.add_value_provider_argument(
            '--output_table',
            type=str,
            help='BigQuery table to write the results to'
        )


class ParseCSVRow(beam.DoFn):
    def process(self, element):
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
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read CSV' >> beam.io.ReadFromText(custom_options.input_csv_file, skip_header_lines=1)
            | 'Parse CSV Rows' >> beam.ParDo(ParseCSVRow())
            | 'Group by Employee ID' >> beam.GroupByKey()
            | 'Compute First Login and Last Logout' >> beam.ParDo(ComputeFirstLoginLastLogout())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                custom_options.output_table,
                schema='employee_id:STRING, name:STRING, department:STRING, first_login:STRING, last_logout:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == '__main__':
    run()
