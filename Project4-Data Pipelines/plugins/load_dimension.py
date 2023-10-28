from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
    """

    truncate_sql = """
        TRUNCATE TABLE {};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql="",
                 table="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.table))
        redshift_hook.run(LoadDimensionOperator.insert_sql.format(self.table,self.sql))
