import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.aws_s3_pipeline import upload_s3_pipeline
from pipelines.youtube_pipeline import youtube_pipeline

default_args = {
    'owner': 'Jihyun',
    'start_date': datetime(2024, 12,15)

}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id = 'etl_youtube_pipeline',
    default_args= default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['youtube', 'etl','pipeline']
)

#extraction from youtube
extract = PythonOperator(
    task_id = 'youtube_extraction',
    python_callable=youtube_pipeline,
    op_kwargs={
        'file_name' : f'youtube_{file_postfix}',
        'time_filter': 'day',
        'limit' : 100
    },
    dag=dag
)

#upload to s3
upload_s3 = PythonOperator(
    task_id = 's3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)