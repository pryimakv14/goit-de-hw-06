from kafka import KafkaConsumer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    "alerts_output_vp5",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    group_id="alert_display_group",
    auto_offset_reset='earliest'
)

for message in consumer:
    alert = message.value
    print(f"Received alert: {alert}")
