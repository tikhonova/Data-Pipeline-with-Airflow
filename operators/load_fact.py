from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers import SqlQueries
import boto3

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 append=False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append = append

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if not self.append:
            self.log.info("Delete {} fact table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Insert Data into Facts table {}".format(self.table))
        formatted = getattr(SqlQueries, self.sql).format(self.table)
        redshift.run(formatted)
