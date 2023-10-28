from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_checks = data_checks


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.data_checks:
            sql = check['sql']
            expected_result = check['result']

            records = redshift_hook.get_records(sql)[0]

            if expected_result != records[0]:
                raise ValueError('Data quality check failed')
