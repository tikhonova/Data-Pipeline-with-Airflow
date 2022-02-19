from datetime import datetime, timedelta
from airflow import DAG
from operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator,  DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

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

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag)

stage_songs_to_redshift = StageToRedshiftOperator(
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="s3://udacity-dend/song_data",
    #s3_key="song_data/A/B/C/TRABCEI128F424C983.json",
    extra_params="json 'auto' compupdate off region 'us-west-2'",
    task_id='Stage_songs',
    dag=dag)

load_songplays_table = LoadFactOperator(
    dag=dag,
    task_id='Load_songplays_table',
    sql= SqlQueries.songplay_table_insert,
    conn_id = 'redshift',
    table = 'songplays',
    mode='append' )

load_user_dimension_table = LoadDimensionOperator(
        dag=dag,
        task_id='Load_user_table',
        conn_id = 'redshift',
        sql = SqlQueries.user_table_insert,
        table = 'users',
        mode="truncate"
    )

load_song_dimension_table = LoadDimensionOperator(
        dag=dag,
        conn_id = 'redshift',
        task_id='Load_song_table',
        sql = SqlQueries.song_table_insert,
        table = 'songs',
        mode="truncate"
    )

load_artist_dimension_table = LoadDimensionOperator(
        dag=dag,
        conn_id = 'redshift',
        task_id='Load_artist_table',
        sql = SqlQueries.artist_table_insert,
        table = 'artists',
        mode="truncate"
    )

load_time_dimension_table = LoadDimensionOperator(
        dag=dag,
        conn_id = 'redshift',
        task_id='Load_time_table',
        sql = SqlQueries.time_table_insert,
        table = 'time',
        mode="truncate"
    )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = "redshift",
    tables = ['songplays', 'users', 'songs', 'artists', 'time'],
    sql="SELECT count(*) FROM {}",
    fail_result=0
)
# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_songplays_table \
>> [load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table] \
>> run_quality_checks