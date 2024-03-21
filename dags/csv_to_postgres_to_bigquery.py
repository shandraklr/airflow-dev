import pendulum
import pandas as pd
from datetime import timedelta

from airflow import DAG
from airflow.models import XCom
from airflow.decorators import task
from airflow.utils.db import provide_session
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

doc_md = """
load data from local csv >> potgresql >> gcs >> bigquery
"""

MY_BUCKET = "slr-bucket-terraform"
MY_DATASET = "slr_dataset_terraform"

@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag.dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

default_args = {
    "owner" : "shadrak",
    "retries" : 2,
    "retry_delay" : timedelta(seconds=2)
}

with DAG(
    dag_id = "csv_postgres_bigquery",
    default_args = default_args,
    start_date = pendulum.datetime(2023, 1, 1, tz = "Asia/Jakarta"),
    schedule_interval = "@once",
    catchup = False,
    tags = ['csv', 'postgres', 'bigquery']
):

    start = EmptyOperator(task_id='start')

    @task
    def read_and_load_dataset():

        destination = PostgresHook(postgres_conn_id = "postgres_local")
        dest_engine = destination.get_sqlalchemy_engine()

        today = pendulum.now(tz = "Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")

        df = pd.read_csv("/usr/local/airflow/include/files/spotify_long_tracks_2014_2024.csv", sep=",")

        ## cleaning columns names
        df.columns = map(str.lower, df.columns)

        df["loaded_at"] = today

        df.columns = df.columns.str.replace(f"\s", "_", regex = True) \
                               .str.replace("[()]", "", regex = True)
        
        df.to_sql(
            "spotify_long_hits",
            dest_engine,
            index = False,
            if_exists = "replace",
            schema = "dl",
            chunksize = 1000
        )
    
    gcs_load = PostgresToGCSOperator(
        task_id = "load_data_to_gcs",
        gcp_conn_id = "gcs_belajar",
        postgres_conn_id = "postgres_local",
        sql = "select * from dl.spotify_long_hits where duration_minutes >= 40;",
        bucket = MY_BUCKET,
        filename = "spotify_long_hits.csv",
        export_format = "csv",
        field_delimiter = ",",
        gzip = "False",
        use_server_side_cursor = "False"
    )

    bq_load = GCSToBigQueryOperator(
        task_id = "load_data_to_bigquery",
        bucket = MY_BUCKET,
        source_objects = ['spotify_*.csv'],
        source_format = "CSV",
        skip_leading_rows = 1,
        destination_project_dataset_table = F"{MY_DATASET}.spotify_long_hits",
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_TRUNCATE",
        gcp_conn_id = "gcs_belajar",
        cluster_fields = "artists"
    )

    cleaning_cache = python_task = PythonOperator(
        task_id = "cleaning_cache",
        python_callable = cleanup_xcom,
        provide_context = True
    )

    end = EmptyOperator(task_id='end')

    start >> read_and_load_dataset() >> gcs_load >> bq_load >> cleaning_cache >> end