from confluent_kafka.admin import AdminClient, NewTopic

# Set up Kafka client
admin_client = AdminClient({"bootstrap.servers": "localhost:29092"})

topics = [
    NewTopic('logging_info',num_partitions=3, replication_factor=1),
    NewTopic('agg_logging_info',num_partitions=3, replication_factor=1)
]

fs = admin_client.create_topics(topics)

for topic,future in fs.items():
    try:
        future.result()
        print(f"Topic {topic} has been created successfully")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")