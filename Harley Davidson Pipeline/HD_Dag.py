from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import requests

sub_urls = [
    "Harley_Davidson_Customer_Lookup.csv",
    "Harley_Davidson_Product_Categories_Lookup.csv",
    "Harley_Davidson_Product_Lookup.csv",
    "Harley_Davidson_Product_Subcategories_Lookup.csv",
    "Harley_Davidson_Returns_Data.csv",
    "Harley_Davidson_Sales_Data_2020.csv",
    "Harley_Davidson_Sales_Data_2021.csv",
    "Harley_Davidson_Sales_Data_2022.csv",
    "Harley_Davidson_Territory_Lookup.csv"
]

def ingest_data_to_gcs():
    run_date = datetime.now().date().isoformat()
    for urls in sub_urls:
        url = f"https://github.com/soumya-569/Project-Essentials/raw/refs/heads/project/Project%20Data/Harley_Davidson/{urls}"
        response = requests.get(url=url,stream=True,timeout=(10,120))

        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        client = gcs_hook.get_conn()
        bucket = client.bucket("harley-davidson-data")
        object_name = f"Harley_Davidson/{run_date}/{urls}"
        blob = bucket.blob(object_name)

        blob.chunk_size = 256 * 1024  # 256 KB

        with blob.open("wb") as load_data:
            for chunk in response.iter_content(chunk_size=256*1024):
                if chunk:
                    load_data.write(chunk)

default_args = {
    "owner":"sunny",
    "retries":5,
    "retry_delay":timedelta(minutes=1)
}

with DAG(
    dag_id="harley_davidson_pipeline",
    default_args = default_args,
    description = "This dag helps to ingest data from api into GCS & transform in spark on a Dataproc cluster and finally save it in Bigquery",
    start_date = datetime(2025,12,14),
    schedule_interval = "0 0 * * *",
    tags=["GCP","GCS","Dataproc","Spark","Bigquery"]
) as dag:
    
    ingest_into_gcs = PythonOperator(
        task_id="api_to_gcs",
        python_callable=ingest_data_to_gcs
    )

    transform_load_bq = DataprocSubmitJobOperator(
        task_id="load_transform_gcs_bq",
        project_id = "budget-cloud-465616",
        region = "asia-south1",
        gcp_conn_id = "google_cloud_default",
        job = {
            "reference":{"project_id":"budget-cloud-465616"},
            "placement":{"cluster_name":"harley-davidson-cluster"},
            "pyspark_job":{
                "main_python_file_uri":"gs://harley-davidson-data/Python_File/Harley-Davidson-Sales.py"
            }
        }
    )

    ingest_into_gcs >> transform_load_bq


