from kafka.admin import KafkaAdminClient, NewTopic
from config import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

topics = [
    NewTopic(name="nata_athlete_event_results", num_partitions=2, replication_factor=1),
    NewTopic(name="nata_athlete_aggregation", num_partitions=1, replication_factor=1),
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created")
except Exception as e:
    print(f"Error creating topics: {e}")

for t in admin_client.list_topics():
    if t.startswith("nata_"):
        print("Topic:", t)

admin_client.close()
