from airflow import DAG
from airflow.operators.python import PythonOperator
from neo4j import GraphDatabase
from faker import Faker
import random
from datetime import datetime

default_args = {
    'depands_on_past': False,
    'start_date': datetime(2025, 7, 13),
}

fake = Faker()

uri = 'bolt://neo4j:7687'
user = 'neo4j'
password = 'password'

def generate_and_insert_to_neo4j():
    driver = GraphDatabase.driver(uri, auth=(user, password))

    def insert(tx, transaction_id, transaction_type, amount, currency):
        tx.run("CREATE (t:Transaction {transaction_id: $transaction_id, transaction_type: $transaction_type, amount: $amount, currency: $currency})", 
               transaction_id=transaction_id, 
               transaction_type=transaction_type, 
               amount=amount, 
               currency=currency)

    with driver.session() as session:
        for i in range(80):
            transaction_id = fake.uuid4()
            transaction_type = random.choice(['deposit', 'tansfer', 'payment',])
            amount = round(random.uniform(10, 10000), 2)
            currency = random.choice(['UDS', 'EUR', 'RUB',])
            session.execute_write(insert, transaction_id, transaction_type, amount, currency)

    driver.close()

with DAG(
    'NEO4J_generate_data',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    generate_and_load_to_neo4j = PythonOperator(
        task_id='generate_and_load_to_neo4j',
        python_callable=generate_and_insert_to_neo4j
    )

    generate_and_load_to_neo4j
