from datetime import datetime, timedelta
import os
from airflow import DAG
#from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import boto3

# Default Arguments per Project Requirements
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 1, 1),
    'end_date': datetime(2022, 3, 1),
    'depends_on_past': False,  # DAG should not have dependencies on past runs
    'retries': 3,  # on failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5),  # retries must happen every 5 minutes
    'catchup_by_default': False,  # catchup is turned off
    'email_on_retry': False  # no email on retry
}

dag = DAG('redshift_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )

# start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_key='log_data',
    s3_bucket='udacity-dend',
    json_path='s3://udacity-dend/log_json_path.json',
    redshift_conn_id="redshift",
    aws_credentials_id='aws_credentials',
    region="us-west-2",
    data_format="JSON",
    provide_context=True,
    truncate_table=True

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_key='log_data',
    s3_bucket='udacity-dend',
    json_path='s3://udacity-dend/song_data',
    redshift_conn_id="redshift",
    aws_credentials_id='aws_credentials',
    region="us-west-2",
    data_format="JSON",
    provide_context=True,
    truncate_table=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplay",
    sql="songplay_table_insert",
    append=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql="user_table_insert",
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="song",
    sql="song_table_insert",
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artist",
    sql="artist_table_insert",
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql="time_table_insert",
    append=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplay", "users", "song", "artist", "time"]
)

# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table]
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
