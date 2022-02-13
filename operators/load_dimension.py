from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperators
from helpers import SqlQueries
import boto3

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append:
            self.log.info("Delete {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Insert data from fact table into {} dimension table".format(self.table))
        formatted = getattr(SqlQueries, self.sql).format(self.table)
        redshift.run(formatted)
