from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import logging
import os
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def extract_session_data(**kwargs):
    
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql('select * from raw.sessions', engine)

    print(data)
    kwargs['ti'].xcom_push(key='session_data', value=data)


def extract_events_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql('select * from raw.events', engine)

    print(data)
    kwargs['ti'].xcom_push(key='events_data', value=data)

def merge_data(**kwargs):
    sessions = kwargs['ti'].xcom_pull(key='session_data', task_ids='data_extraction.extract_session_data')

    events = kwargs['ti'].xcom_pull(key='events_data', task_ids='data_extraction.extract_events_data')

    sessions = sessions.values.tolist()

    events = events.values.tolist()

    session_dict = {}
    for sess in sessions:
        session_id = sess[0]
        session_dict[session_id] = {
            'duration_sec': sess[5],
            'event_count': 0,
            'session_date': sess[2]
        }

    for event in events:
        session_id = event[1]
        if session_id in session_dict:
            session_dict[session_id]['event_count'] += 1

    result = [
        (sess_id, data['duration_sec'], data['event_count'])
        for sess_id, data in session_dict.items()
    ]

    print(result)
    kwargs['ti'].xcom_push(key='joined_data', value=result)

def remove_duplicates(**kwargs):
    data = kwargs['ti'].xcom_pull(key='joined_data', task_ids='merge_session_data')
    df = pd.DataFrame(data, columns=['session_id', 'duration_sec', 'event_count'])

    df.drop_duplicates(inplace=True)

    kwargs['ti'].xcom_push(key='clean_data', value=df.values.tolist())


def validate_non_zero_stats(**kwargs):
    data = kwargs['ti'].xcom_pull(key='clean_data', task_ids='data_quality_checks.remove_duplicates')
    df = pd.DataFrame(data, columns=['session_id', 'duration_sec', 'event_count'])

    if (df['duration_sec'] == 0).any() or (df['event_count'] == 0).any():
        raise ValueError("Некоторые записи имеют duration_sec или event_count = 0")

    kwargs['ti'].xcom_push(key='validated_data', value=df.values.tolist())



def load_to_temp_table(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='validated_data', task_ids='data_quality_checks.validate_non_zero_stats')
    print(data)

    hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    hook.run("DELETE FROM temp.session_event_stats", autocommit=True)

    for row in data:
        hook.run(f"""
            INSERT INTO temp.session_event_stats VALUES (
                '{row[0]}', '{row[1]}', '{row[2]}'
            )
        """, autocommit=True)


with DAG(
    'Extract_and_prepair_data',
    description = 'Извлечение и подготовка данных',
    schedule_interval = '@daily',
    catchup = False,
    tags = ['raw'],
    start_date = datetime(2025, 7, 7)
) as dag:
    
    with TaskGroup('data_extraction') as extraction_group:

        extract_data_sessions_task = PythonOperator(
            task_id = 'extract_session_data',
            python_callable = extract_session_data,
            provide_context = True
        )

        extract_data_events_task = PythonOperator(
            task_id = 'extract_events_data',
            python_callable = extract_events_data,
            provide_context = True
        )

        extract_data_sessions_task >> extract_data_events_task
    

    merge_data_task = PythonOperator(
        task_id = 'merge_session_data',
        python_callable = merge_data,
        provide_context = True
    )
    
    with TaskGroup('data_quality_checks') as data_quality_checks:

        remove_duplicates_task = PythonOperator(
            task_id='remove_duplicates',
            python_callable=remove_duplicates,
            provide_context=True
        )

        validate_non_zero_stats_task = PythonOperator(
            task_id='validate_non_zero_stats',
            python_callable=validate_non_zero_stats,
            provide_context=True
        )

        remove_duplicates_task >> validate_non_zero_stats_task

    
    joined_data_task = PythonOperator(
        task_id = 'joined_data_task',
        python_callable = load_to_temp_table,
        provide_context = True
    )

    trigger_dag = TriggerDagRunOperator(
        task_id = 'trigger_DAG2',
        trigger_dag_id = 'agg_sessions',
        execution_date = '{{execution_date}}',
        wait_for_completion = False,
        reset_dag_run = True,
        trigger_rule = 'all_success'
    )

    extraction_group >> merge_data_task >> data_quality_checks >> joined_data_task >> trigger_dag