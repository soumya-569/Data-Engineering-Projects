from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
from datetime import datetime,timedelta
import requests

sub_urls = [
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv"
]

def ingest_into_s3():
    run_date = datetime.now().date().isoformat()
    s3_hook = S3Hook(aws_conn_id="aws_default")
    client = s3_hook.get_conn()
    chunk_size = 8 *1024 * 1024  # 8MB
    for url in sub_urls:
        data_url = f"https://raw.githubusercontent.com/soumya-569/Project-Essentials/refs/heads/project/Project%20Data/olist_ecommerce/{url}"

        response = requests.get(url=data_url,stream=True,timeout=(10,120))
        response.raw.decode_content = True

        object_name = f"Bronze/{run_date}/{url}"

        client.upload_fileobj(
            Fileobj = response.raw,
            Bucket = "amz-s3-databricks-conn",
            Key = object_name,
            Config=boto3.s3.transfer.TransferConfig(
                multipart_chunksize = chunk_size,
                multipart_threshold= chunk_size 
            )
        )

default_args = {
    "owner":"sunny",
    "retries":5,
    "retry_delay":timedelta(minutes=1)
}

with DAG(
    dag_id = "olist_ecommerce_pipeline",
    default_args = default_args,
    description = "This dag helps to retrieve data from api and ingest into Bronze layer in S3 and transform it in spark on Databricks and finally load it into Silver layer in S3",
    start_date = datetime(2025,12,15),
    schedule_interval = "0 0 * * *",
    tags = ["AWS","S3","Databricks","ETL"]
) as dag:
    
    task1 = PythonOperator(
        task_id = "api_to_s3",
        python_callable = ingest_into_s3
    )

    task2 = DatabricksRunNowOperator(
        task_id = "transform_load_s3",
        databricks_conn_id = "databricks_free_edition_default",
        job_id = 403894378248867
    )

    task1 >> task2
