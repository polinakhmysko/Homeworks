import json
import psycopg2
from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'temperature_cosumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['topic_temperature'])

db_conn = psycopg2.connect(
    host='localhost',
    database='testdb',
    user='admin',
    password='secret',
    port='5432'
)

cursor = db_conn.cursor()

def insert_into_postgres(data):
    query = """
                INSERT INTO temperature_readings (timestamp, value)
                VALUES (%s, %s)
            """
    cursor.execute(query, (data['timestamp'], data['value']))
    db_conn.commit()

try:
    print("[v] Консьюмер начал работать..")
    while True:
        msg = consumer.poll(timeout=0.01)
        
        if msg is None:
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            print(f'[v] Сообщение получено: {data}')
            insert_into_postgres(data)
        except:
            print('[x] Ошибка')

except KeyboardInterrupt:
    print("[x] Консъюмер остановлен")

finally:
    consumer.close()