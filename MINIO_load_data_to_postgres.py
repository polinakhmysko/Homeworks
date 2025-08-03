from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def get_and_load():
    hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'data-bucket'
    minio_client = hook.get_conn()

    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()

    # получаем уже обработанные ключи файлов
    with engine.connect() as conn:
        result = conn.execute("SELECT file_key FROM raw.processed_files")
        processed_keys = {row[0] for row in result}

    response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix='raw/')

    for obj in response['Contents']:
        filekey=obj['Key']

        if filekey in processed_keys:
            continue  # пропустить уже обработанный файл

        obj_data=minio_client.get_object(Bucket=bucket_name, Key=filekey)
        df = pd.read_csv(obj_data['Body'])
        df.to_sql('minio_data', engine, schema = 'raw', if_exists='append', index=False)

        # добавляем ключ в таблицу processed_files
        with engine.begin() as conn:
            conn.execute(
                "INSERT INTO raw.processed_files (file_key) VALUES (%s) ON CONFLICT DO NOTHING",
                (filekey,)
            )

with DAG(
    'MINIO_load_data_to_postgres',
    start_date = datetime(2025, 7, 15),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    transfer_task = PythonOperator(
        task_id='transfer_task',
        python_callable=get_and_load
    )

    transfer_task