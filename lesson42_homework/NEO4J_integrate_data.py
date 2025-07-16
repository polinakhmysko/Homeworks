from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from neo4j import GraphDatabase
from datetime import datetime
import logging

default_args = {
    'depands_on_past': False,
    'start_date': datetime(2025, 7, 13),
}

uri = 'bolt://neo4j:7687'
user = 'neo4j'
password = 'password'

def transfer_from_neo4j_to_postgres():
    driver = GraphDatabase.driver(uri, auth=(user, password))
    query = """
    MATCH (t:Transaction)
    RETURN t.transaction_id AS transaction_id, t.transaction_type AS transaction_type, t.amount AS amount, t.currency AS currency
    """
    with driver.session() as session:
        result = session.run(query)
        data = [(record['transaction_id'], record['transaction_type'], record['amount'], record['currency']) for record in result]
    driver.close()

    if data:
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for record in data:
            cursor.execute('''
                INSERT INTO raw.bank_transactions_neo4j values (%s,%s,%s,%s)
                ON CONFLICT (transaction_id) DO NOTHING
            ''', record
            )

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data inserted successfully")

with DAG(
    'NEO4J_integrate_date',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    transfer_from_neo4j = PythonOperator(
        task_id='transfer_from_neo4j',
        python_callable=transfer_from_neo4j_to_postgres
    )

    transfer_from_neo4j