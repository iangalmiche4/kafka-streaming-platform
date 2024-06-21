from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    data = {
        'ip': f'192.168.0.{random.randint(1, 255)}',
        'url': f'/index{random.randint(1, 10)}.html',
        'response_time': random.randint(1, 500)
    }
    producer.send('web-logs', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
