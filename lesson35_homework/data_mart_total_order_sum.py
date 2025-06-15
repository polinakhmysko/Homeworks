from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': "polina",
}

with DAG(
    "user_total_order_sum",
    default_args = default_args,
    description = 'Определение общей суммы заказов каждого пользователя',
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    user_total_order_amount = PostgresOperator(
        task_id="user_total_order_amount",
        postgres_conn_id="my_postgres_conn",
        sql="""
        INSERT INTO data_mart.user_order_amount (name, surname, card_number, total_order_amount)
        SELECT 
            u.name,
            u.surname,
            u.card_number,
            SUM(o.total_price) AS total_order_amount
        FROM raw.users u
        JOIN raw.orders o ON o.card_number = u.card_number
        GROUP BY u.card_number, u.name, u.surname
        ;
        """,
    )

user_total_order_amount