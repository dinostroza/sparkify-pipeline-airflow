from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    load_template_sql = """
        INSERT INTO {dim_table} 
        {insert_select_sql}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 insert_select_sql="",
                 dim_table="",
                 delete=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.insert_select_sql = insert_select_sql
        self.dim_table = dim_table
        self.delete = delete

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete:
            self.log.info("Clearing data from dimension table")
            redshift.run("TRUNCATE TABLE {}".format(self.dim_table))

        self.log.info('Loading data to dimension table')
        load_sql = LoadDimensionOperator.load_template_sql.format(
            dim_table=self.dim_table,
            insert_select_sql=self.insert_select_sql
        )
        redshift.run(load_sql)