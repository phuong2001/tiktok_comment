from kafka import KafkaConsumer
import json

BOOTSTRAP = "192.168.1.28:9092"
TOPIC = "comment_jobs_success"

c = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",    # đọc từ đầu
    enable_auto_commit=False,        # đừng commit để test lặp
    group_id="test-success-checker-"+__import__("time").strftime("%H%M%S"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8", errors="ignore")),
)

print(f"[*] Listening {TOPIC} from-beginning with a fresh group...")
for m in c:
    print("[✓] got:", m.topic, "p", m.partition, "@", m.offset, m.value)
