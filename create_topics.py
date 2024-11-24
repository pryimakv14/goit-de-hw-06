from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
)

building_sensors_topic = NewTopic(name="building_sensors_vp5", num_partitions=2, replication_factor=1)
alerts_output_topic = NewTopic(name="alerts_output_vp5", num_partitions=2, replication_factor=1)
admin_client.create_topics(new_topics=[building_sensors_topic, alerts_output_topic], validate_only=False)

admin_client.close()
