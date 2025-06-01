import json
from confluent_kafka import Consumer, Producer, KafkaException

consumer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'cosumer_group',
    'auto.offset.reset': 'earliest'
}

producer_conf = {
    'bootstrap.servers': 'localhost:9094'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe(['raw_data'])

try:
    print("[v] Консъюмер начал работать..")
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            print(f'[v] Сообщение получено: {data}')
            msg_type = data.get('type')
            if msg_type == 'temperature':
                producer.produce('topic_temperature', value=json.dumps(data))
            elif msg_type == 'humidity':
                producer.produce('topic_humidity', value=json.dumps(data))
            producer.poll(0)
        except:
            print('[x] Ошибка')

except KeyboardInterrupt:
    print("[x] Консьюмер остановлен")

finally:
    consumer.close()
    producer.flush()