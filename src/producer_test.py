from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="192.168.1.28:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

msg = {
    "url": "https://www.tiktok.com/@elz.study/video/7554793319776128263",
    "comments": ["hello", "bot test", "quá hay"]
}

producer.send("tiktok-platforms", value=msg)
producer.flush()
print("Sent:", msg)
