import pendulum
from datetime import timedelta

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from airflow import DAG
from airflow.models import XCom
from airflow.utils.db import provide_session
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

doc_md = """
load data from local csv to potgresql and load to bigquery
"""

MY_BUCKET = "slr-bucket-terraform"
MY_DATASET = "slr_dataset_terraform"
POSTGRES_CONN_ID = "postgres_local"

today_ = pendulum.now(tz = "Asia/Jakarta").format("YYYY-MM-DD")

@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag.dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

default_args = {
    "owner" : "robot",
    "retries" : 2,
    "retry_delay" : timedelta(seconds=2)
}

with DAG(
    dag_id = "csv_postgres_bigquery_seconds",
    default_args = default_args,
    start_date = pendulum.datetime(2023, 1, 1, tz = "Asia/Jakarta"),
    schedule_interval = "@once",
    template_searchpath = "/usr/local/airflow/include/sql/",
    catchup = False,
    tags = ['csv', 'postgres', 'bigquery']
):

    start = EmptyOperator(task_id='start')

    cleaning_tables = PostgresOperator(
        task_id = "cleaning_table",
        sql = "cleaning_tables_spotify.sql",
        postgres_conn_id = POSTGRES_CONN_ID,
    )

    postgres_load = aql.load_file(
        task_id = "load_csv_to_postgres",
        input_file = File(path = "/usr/local/airflow/include/files/spotify_long_tracks_2014_2024.csv"),
        output_table = Table(
            conn_id = POSTGRES_CONN_ID,
            metadata = Metadata(schema = "dl"),
            name = "spotify_long_hits"
        ),
        if_exists = "append",
    )
    
    gcs_load = PostgresToGCSOperator(
        task_id = "load_data_to_gcs",
        gcp_conn_id = "gcs_belajar",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql = "select distinct * from dl.spotify_long_hits where duration_minutes >= 40;",
        bucket = MY_BUCKET,
        filename = f"spotify_long_hits_{today_}.csv",
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

    end = EmptyOperator(task_id='end')

    start >> cleaning_tables >> postgres_load >> gcs_load >> bq_load >> end