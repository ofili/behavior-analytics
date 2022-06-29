#!/usr/bin/python

# Import the standard library
import os
import json

from datetime import datetime, timedelta

from utils import _local_to_s3, run_redshift_external_query

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator 
from airflow.contrib.sensor.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

# Environment variables
env_variables = Variable.get("env_variables", deserialize_json=True)
BUCKET_NAME = env_variables("BUCKET")
EMR_ID = env_variables("EMR_ID")
EMR_STEPS = {}
with open("./dags/scripts/clean_movie_review.json") as f:
    EMR_STEPS = json.load(f)

# DAG definition
default_args = {
    "owner": "Lewis",
    "depends_on_past": True,
    "start_date": datetime(2022, 1, 11),
    "email": ["ofililewis@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "provide_context": True,
}

dag = DAG(
    "user_behavior",
    default_args=default_args,
    schedule_interval="0 0 * * *", # Run every day at midnight
    max_active_runs=1,
    catchup=False,
    description="User behavior analysis",
    tags=["user_behavior"],
)

extract_user_purchase_data = PostgresOperator( # Extract user purchase data to S3
    task_id="extract_user_purchase_data",
    postgres_conn_id="postgres_default",
    sql="./scripts/sql/unload_user_purchase.sql", # Unload user purchase data from Postgres
    dag=dag,
    params={"user_purchase": "temp/user_purchase.csv"}, # S3 path for user purchase data
    depends_on_past=True, # Run this task after the previous task
    wait_for_downstream=True, # Wait for the previous task to finish
)

user_purchase_to_stage_data_lake = PythonOperator( # Load user purchase data to Data Lake 
    task_id="user_purchase_to_stage_data_lake", # Task ID
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/opt/airflow/temp/user_purchase.csv", # Local path for user purchase data from Postgres unload query (temp/user_purchase.csv)
        "s3_file": "user_purchase.csv",
        "bucket_name": BUCKET_NAME,
        "remove_local": "true", # Remove local file after upload to S3 (true)
        },
    dag=dag,
)

user_purchase_to_stage_data_lake_to_stage_table = PythonOperator( # Load user purchase data to Data Lake staging table (user_purchase_stage)
    task_id="user_purchase_to_stage_data_lake_to_stage_table",
    python_callable=run_redshift_external_query,
    op_kwargs={
        "query": "ALTER TABLE spectrum.user_purchase_stg ADD IF NOT EXISTS PARTITION (dt='{{ ds }}') LOCATION 's3://{{ bucket_name }}/stage/user_purchase/dt={{ ds }}/';", # Load user purchase data to Data Lake staging table
        "bucket_name": BUCKET_NAME,
        # "remove_local": "true",
        },
    dag=dag,
)

movie_review_to_raw_data_lake = PythonOperator( # Load movie review data to Data Lake raw table
    task_id="movie_review_to_raw_data_lake",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "/opt/airflow/data/movie_review.csv", # Local path for movie review data to load to Data Lake raw table
        "key": "raw/movie_review/{{ ds }}/movie.csv",
        "bucket_name": BUCKET_NAME,
    },
    dag=dag,
)

spark_script_to_s3 = PythonOperator( # Run Spark script to load movie review data to Data Lake raw table
    task_id="spark_script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={
        "file_name": "./dags/scripts/spark/random_text_classification.py", # This is the spark script to run on EMR cluster (./dags/scripts/spark/random_text_classification.py)
        "key": "spark/random_text_classification.py", # This is the key to store the script on S3 (spark/random_text_classification.py)
        "bucket_name": BUCKET_NAME,
    },
)

start_emr_movie_classification_script = EmrAddStepsOperator( # Start Spark script to load movie review data to Data Lake raw table
    task_id="start_emr_movie_classification_script",
    job_flow_id=EMR_ID,
    aws_conn_id="aws_default",
    steps=EMR_STEPS,
    dag=dag,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "raw_movie_review": "raw/movie_review/{{ ds }}/movie.csv", #This file is created by the previous operator
        "text_classifier_script": "spark/random_text_classification.py", #This file is created by the previous operator
        "stage_movie_review": "stage/movie_review/{{ ds }}/movie.csv",	# This is the file that will be loaded into the data lake
    },
    depends_on_past=True,
)

last_step = len(EMR_STEPS) - 1 # The last step in the EMR script is the Spark script to load movie review data to Data Lake raw table

wait_for_movie_classification_transformation = EmrStepSensor( # Wait for the Spark script to load movie review data to Data Lake raw table
    dag=dag,
    task_id="wait_for_movie_classification_transformation",
    job_flow_id=EMR_ID,
    step_id='{{ task_instance.xcom_pull("start_emr_movie_classification_script", key="steps")[{}].id }}'.format(last_step),
    aws_conn_id="aws_default",
    depends_on_past=True,
)
'''   ("start_emr_movie_classification_script", key="return_value")['
+ str(last_step)
+ "] }}", '''


generate_user_behavior_metric = PostgresOperator( 
    task_id="generate_user_behavior_metric",
    postgres_conn_id="redshift",
    sql="scripts/sql/generate_user_behavior_metric.sql", 
    dag=dag,
)

end_of_data_pipeline = DummyOperator( # End of data pipeline
    task_id="end_of_data_pipeline",
    dag=dag,
)

(
    extract_user_purchase_data >> 
    user_purchase_to_stage_data_lake >>
    user_purchase_to_stage_data_lake_to_stage_table
)

(
    [
        movie_review_to_raw_data_lake,
        spark_script_to_s3,
    ]
    >> start_emr_movie_classification_script
    >> wait_for_movie_classification_transformation
)

(
    [
        user_purchase_to_stage_data_lake_to_stage_table,
        wait_for_movie_classification_transformation,
    ]
    >> generate_user_behavior_metric
    >> end_of_data_pipeline
)