import json
import time

from kafka import KafkaProducer, KafkaConsumer

# kafka producer init
TOPIC_IN = "messages"
TOPICS_OUT = "predictions"
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"],
                         auto_offset_reset="latest",
                         value_deserializer=lambda x: json.loads(x.decode("utf-8")))
consumer.subscribe([TOPICS_OUT])

print("Sending messages...")
for msg in [
    {"id": 0, "clean_text": "This product is really nice."},
    {"id": 1, "clean_text": "This table was horrible."},
    {"id": 2, "clean_text": "Hello!."},
    {"id": 3, "clean_texxx": "sdasd"},  # this message will be sent to the error topic!
]:
    print(f"sending: {msg}")
    producer.send(topic=TOPIC_IN, value=json.dumps(msg).encode("utf-8"))

print("wait for it...")
time.sleep(1)
print("there!")

for message in consumer:
    obj = message.value
    print(f"Text: '{obj['clean_text']}', prediction: {obj['prediction_label'].upper()}")
