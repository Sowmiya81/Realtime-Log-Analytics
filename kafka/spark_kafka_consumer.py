import json
from decimal import Decimal
import boto3
from confluent_kafka import Consumer, KafkaException, KafkaError
from faker import Faker
fake = Faker('en_US')

dynamodb = boto3.resource('dynamodb',endpoint_url="http://localhost:8000", region_name="us-west-2",  aws_access_key_id="dummy",  aws_secret_access_key="dummy")

table_name = 'agg_system_logs'
existing_tables = dynamodb.tables.all()
table_exists = any(table.name == table_name for table in existing_tables)

if not table_exists:
    print(f"Table {table_name} does not exist creating it ..... ")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        })

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    print(f"Table {table_name} has been created successfully")
else:
    table = dynamodb.Table(table_name)
    print(f"Table {table_name} already exists")


consumer = Consumer({
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'logging_consumer_id',
        'auto.offset.reset': 'earliest',
})


consumer.subscribe(['agg_logging_info'])


def consume_logs():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached : {msg.topic()} {msg.key()} [{msg.partition()}] @offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
            else:
                log_message = json.loads(msg.value().decode('utf-8'))
                print(log_message)

                try:
                    response = table.put_item(
                        Item={
                            'id': str(fake.uuid4()),
                            'api': log_message['api'],
                            'log_level': log_message['log_level'],
                            'log_count': log_message['log_count'],
                            'avg_response_time': Decimal(str(log_message['avg_response_time'])),
                            'max_response_time': Decimal(str(log_message['max_response_time'])),
                            'min_response_time': Decimal(str(log_message['min_response_time'])),
                            'window_start': log_message['window_start'],
                            'window_end': log_message['window_end']

                        }
                    )
                    print(f"Log saved to DynamoDB: {response}")
                except Exception as e:
                    print(f"Error saving log to DynamoDB: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

consume_logs()




