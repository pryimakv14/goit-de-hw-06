from kafka import KafkaProducer
from datetime import datetime
import json
import random
import time
from configs import kafka_config

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
)

sensor_id = random.randint(1000, 9999)

while True:
    data = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": random.randint(25, 45),
        "humidity": random.randint(15, 85)
    }
    producer.send("building_sensors_vp5", value=data)
    print(f"Sent data: {data}")
    time.sleep(5)
