from kafka import KafkaProducer, KafkaConsumer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode()
)
producer.send("eia_events", {"event":"test","domain":"General Dataset"})
producer.flush()

consumer = KafkaConsumer(
    "eia_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode())
)

for msg in consumer:
    print("Received:", msg.value)
    break
