from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
import boto3


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} came back empty".format(table))
                raise ValueError("Failure: {} came back empty".format(table))
            num_records = records[0][0]
            if num_records == 0:
                self.log.error("No records in source table: {}".format(table))
                raise ValueError("No records in source table: {}".format(table))
            self.log.info("Data Quality check passed on {} returning {} rows".format(table, num_records))
