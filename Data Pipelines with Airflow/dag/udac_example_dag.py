from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator

#from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)

# debug sugestion https://knowledge.udacity.com/questions/55812
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

#/opt/airflow/start.sh
#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'Depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )
dag.doc_md = """
#### DAG Summary
Perform ETL. 
Extract data from S3 to staging in redshift, transform into Facts and Dimension tables, and check data quality.

"""

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# sugestion from https://knowledge.udacity.com/questions/163614
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,    
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region='us-west-2',
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='staging_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region='us-west-2',
    s3_bucket="udacity-dend",
    s3_key="song_data" 
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_insert=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql_insert=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_insert=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_insert=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_insert=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_primary_dict={'songs':'songid', 'time':'start_time', 'users':'userid', "artists":"artistid"},
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

# To Fact tables
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# To dimenstion tables
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
