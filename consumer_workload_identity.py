# Pseudo, untested. Same consumer, authenticating with workload identity (OAUTHBEARER).
import os
from azure.identity import DefaultAzureCredential
from confluent_kafka import Consumer, KafkaError

NAMESPACE = os.environ["EVENTHUB_NAMESPACE"]
TOPIC = os.environ["EVENTHUB_NAME"]
GROUP = os.environ.get("CONSUMER_GROUP", "$Default")

credential = DefaultAzureCredential()  # uses the AZURE_* env vars injected by the workload identity webhook
SCOPE = f"https://{NAMESPACE}.servicebus.windows.net/.default"


def oauth_cb(_config_str):
    token = credential.get_token(SCOPE)
    return token.token, token.expires_on  # (token string, expiry as epoch seconds)


conf = {
    "bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": oauth_cb,
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
