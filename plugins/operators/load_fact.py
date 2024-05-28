from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    load_template_sql = """
        INSERT INTO {fact_table} 
        {insert_select_sql}        
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 fact_table="",
                 insert_select_sql = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.insert_select_sql = insert_select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading data to fact table')
        load_sql = LoadFactOperator.load_template_sql.format(
            fact_table=self.fact_table,
            insert_select_sql=self.insert_select_sql
        )
        redshift.run(load_sql)