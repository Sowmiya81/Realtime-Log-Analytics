from confluent_kafka import Consumer,KafkaException,KafkaError
import json


consumer = Consumer({
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'logging_consumer_id',
        'auto.offset.reset': 'earliest',
})


consumer.subscribe(['logging_info'])


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
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

consume_logs()