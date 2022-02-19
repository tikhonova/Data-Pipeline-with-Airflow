from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(self,
                 conn_id,
                 tables: [str],
                 sql,
                 fail_result,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.tables = tables
        self.sql = sql
        self.fail_result = fail_result

    def execute(self, context):
        self.log.info('Launching DataQualityOperator')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for table in self.tables:
            sql = self.sql.format(table)
            res = redshift.get_first(sql)[0]

            if res == self.fail_result:
                raise ValueError(f"failed query for {table}, failure {self.fail_result}")