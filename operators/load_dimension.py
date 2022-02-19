from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers import SqlQueries


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    load_dimension_table_insert = """
    INSERT INTO {} {}
    """
    load_dimension_table_truncate = """
    TRUNCATE TABLE {} 
    """

    def __init__(self,
                 sql="",
                 conn_id="",
                 table="",
                 mode="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.sql = sql
        self.conn_id = conn_id
        self.table = table
        self.mode = mode

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if (self.mode == "append"):
            redshift.run(LoadDimensionOperator.load_dimension_table_insert.format(self.table, self.sql))

        if (self.mode == "truncate"):
            redshift.run(LoadDimensionOperator.load_dimension_table_truncate.format(self.table))
            redshift.run(LoadDimensionOperator.load_dimension_table_insert.format(self.table, self.sql))

            # self.log.info(f"Successful completeion of {self.mode} on {self.table}")