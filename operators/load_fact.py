from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    load_fact_table_insert = """
    INSERT INTO {} {}
    """
    load_fact_table_truncate = """
    TRUNCATE TABLE {} 
    """

    def __init__(self,
                 sql="",
                 conn_id="",
                 table="",
                 mode="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.table = table
        self.mode = mode

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if (self.mode == "append"):
            redshift.run(LoadFactOperator.load_fact_table_insert.format(self.table, self.sql))

        if (self.mode == "truncate"):
            redshift.run(LoadFactOperator.load_fact_table_truncate.format(self.table))
            redshift.run(LoadFactOperator.load_fact_table_insert.format(self.table, self.sql))

            # self.log.info(f"Successful completion of {self.mode} on {self.table}")