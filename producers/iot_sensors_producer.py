from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    data = {
        'sensor_id': f'sensor_{random.randint(1, 100)}',
        'temperature': random.uniform(15.0, 30.0),
        'humidity': random.uniform(30.0, 70.0)
    }
    producer.send('iot-sensors', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
