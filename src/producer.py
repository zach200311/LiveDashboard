import os
import json
import time
import random
import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load .env / env-style files
load_dotenv()

# Prefer new-style env, fall back to old
BROKER = os.getenv("KAFKA_BOOTSTRAP") or os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC  = os.getenv("KAFKA_TOPIC", "traffic_raw")

RUN_FOREVER = os.getenv("RUN_FOREVER", "1") == "1"
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000"))
BASE_SLEEP  = float(os.getenv("BASE_SLEEP", "0.5"))

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

LOCATIONS = ["Main&1st", "Broadway", "Oak", "Dallas-NB", "Dallas-SB"]
DIRS = [0, 1]  # 0/1 like buddy repo

print(f"Producing to {BROKER} topic {TOPIC} (Ctrl+C to stop)")

def make_message() -> dict:
    now = datetime.datetime.now()
    return {
        "created_at": now.strftime("%Y-%m-%d"),
        "timestamp":  now.strftime("%m/%d/%Y %I:%M:%S %p"),  # 10/26/2025 04:14:26 PM
        "PeakSpeed":  random.randint(20, 75),
        "Pmgid":      random.randint(10_000, 99_999),
        "Direction":  random.choice(DIRS),
        "Location":   random.choice(LOCATIONS),
        "VehicleCount": random.randint(1, 8),
        "GeneratedAt":  time.time(),
    }

try:
    if RUN_FOREVER:
        while True:
            msg = make_message()
            producer.send(TOPIC, value=msg)
            producer.flush()
            time.sleep(BASE_SLEEP)
    else:
        for _ in range(NUM_RECORDS):
            msg = make_message()
            producer.send(TOPIC, value=msg)
            producer.flush()
            time.sleep(BASE_SLEEP)
finally:
    producer.close()
