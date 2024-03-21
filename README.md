Overview
========

this is just one of many way how to ETL/ELT and in this example I use airflow from astronomer and when I create a bucket in gcs and create a dataset in BigQuery I use terraform.

Project Contents
================

1. ELT local csv >> postgresql db >> gcs >> bigquery
    - [code first version](https://github.com/shandraklr/csv-to-postgres-to-bigquery-airflow/blob/main/dags/csv_to_postgres_to_bigquery.py)
    - [code seconds version](https://github.com/shandraklr/csv-to-postgres-to-bigquery-airflow/blob/main/dags/csv_to_postgres_to_bigquery_seconds.py)
