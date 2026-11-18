# Pseudo, untested. Event Hubs Kafka consumer using a connection string.
import os
from confluent_kafka import Consumer, KafkaError

NAMESPACE = os.environ["EVENTHUB_NAMESPACE"]
TOPIC = os.environ["EVENTHUB_NAME"]
GROUP = os.environ.get("CONSUMER_GROUP", "$Default")
CONN_STR = os.environ["EVENTHUB_CONNECTION_STRING"]

conf = {
    "bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": CONN_STR,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(msg.error())
            continue
        print(msg.topic(), msg.partition(), msg.offset(), msg.value().decode("utf-8"))
finally:
    consumer.close()
