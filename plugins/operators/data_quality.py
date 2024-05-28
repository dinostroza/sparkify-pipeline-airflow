from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "redshift",
                tests_sql=[],
                expected_results=[],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests_sql = tests_sql
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Running data quality tests")
        for index, test_sql in enumerate(self.tests_sql):
            self.log.info(f"Running data quality number {index}.")
            records = redshift.get_records(test_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality number {index} check failed. Test returned no results.")
            actual_result = records[0][0]
            if actual_result != self.expected_results[index]:
                raise ValueError(f"Data quality number {index} check failed. Test result {actual_result} does not match the expected result {self.expected_results[index]}.")
            self.log.info(f"Data quality number {index} check passed with result: {actual_result}.")
