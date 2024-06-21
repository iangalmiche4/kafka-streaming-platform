from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('web-logs', 'iot-sensors', 'financial-transactions',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Value: {message.value}")
