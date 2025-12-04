import os
import json
import time
import datetime as dt
from kafka import KafkaConsumer
import mysql.connector

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP") or os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME   = os.getenv("KAFKA_TOPIC") or os.getenv("TOPIC_NAME", "traffic_raw")
KAFKA_GROUP  = os.getenv("KAFKA_GROUP", "mysql_raw_consumer")

DB_HOST = os.getenv("DB_HOST") or os.getenv("MYSQL_HOST", "richardsonsql.mysql.database.azure.com")
DB_PORT = int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("DB_USER") or os.getenv("MYSQL_USER", "utdsql")
DB_PASS = os.getenv("DB_PASS") or os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD", "Capstone2025!")
DB_NAME = os.getenv("DB_NAME") or os.getenv("MYSQL_DATABASE", "kafka")

BATCH_SIZE = int(os.getenv("BATCH_ROWS", "200"))

db = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    ssl_disabled=False,
)
cursor = db.cursor()

cursor.execute("SELECT DATABASE()")
print("Connected to database:", cursor.fetchone()[0])

cursor.execute("""
CREATE TABLE IF NOT EXISTS raw_data_kafka (
    id INT AUTO_INCREMENT PRIMARY KEY,
    created_at DATE,
    `timestamp` VARCHAR(25),
    peakspeed INT,
    pmgid BIGINT,
    direction INT,
    location VARCHAR(50),
    vehiclecount INT,
    generation_time DOUBLE,
    total_pipeline_time DOUBLE,
    ts_dt DATETIME NULL
)
""")
db.commit()

# widen timestamp if needed
cursor.execute("""
SELECT CHARACTER_MAXIMUM_LENGTH
FROM information_schema.columns
WHERE table_schema=%s AND table_name='raw_data_kafka' AND column_name='timestamp'
""", (DB_NAME,))
row = cursor.fetchone()
if row and (row[0] is None or int(row[0]) < 25):
    cursor.execute("ALTER TABLE raw_data_kafka MODIFY COLUMN `timestamp` VARCHAR(25)")
    print("Widened `timestamp` to VARCHAR(25)")

# add ts_dt if missing
cursor.execute("""
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema=%s AND table_name='raw_data_kafka' AND column_name='ts_dt'
""", (DB_NAME,))
if cursor.fetchone()[0] == 0:
    cursor.execute("ALTER TABLE raw_data_kafka ADD COLUMN ts_dt DATETIME NULL")
    print("Added column ts_dt")
db.commit()

# indexes
def ensure_index(name, cols):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.statistics
        WHERE table_schema=%s AND table_name='raw_data_kafka' AND index_name=%s
    """, (DB_NAME, name))
    if cursor.fetchone()[0] == 0:
        cursor.execute(f"CREATE INDEX {name} ON raw_data_kafka ({cols})")
        print(f"Created index {name} on ({cols})")

for name, cols in [
    ("idx_ts_dt", "ts_dt"),
    ("idx_loc_ts", "location, ts_dt"),
    ("idx_dir_ts", "direction, ts_dt"),
]:
    ensure_index(name, cols)
db.commit()

# ---------- Kafka consumer ----------
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id=KAFKA_GROUP,
    consumer_timeout_ms=5000,  # 5s idle → stop
)

print(f"Consuming from {KAFKA_BROKER} topic {TOPIC_NAME} → MySQL table: raw_data_kafka")

def parse_ts(s: str):
    if not s:
        return None
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except Exception:
            pass
    return None

INSERT_SQL = """
INSERT INTO raw_data_kafka (
  created_at, `timestamp`, peakspeed, pmgid, direction, location, vehiclecount,
  generation_time, total_pipeline_time, ts_dt
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

msg_count = 0
pipeline_start = time.time()

batch = []

for message in consumer:
    now = time.time()
    data = message.value

    gen_time = float(data.get("GeneratedAt", now))
    total_pipeline_time = now - gen_time
    ts_dt = parse_ts(data.get("timestamp", ""))

    row = (
        data.get("created_at"),
        data.get("timestamp"),
        int(data.get("PeakSpeed", 0) or 0),
        int(data.get("Pmgid", 0) or 0),
        int(data.get("Direction", 0) or 0),
        str(data.get("Location", ""))[:50],
        int(data.get("VehicleCount", 0) or 0),
        gen_time,
        total_pipeline_time,
        ts_dt,
    )
    batch.append(row)
    msg_count += 1

    if len(batch) >= BATCH_SIZE:
        cursor.executemany(INSERT_SQL, batch)
        db.commit()
        print(f"Committed {len(batch)} rows (total {msg_count})")
        batch.clear()

# flush remaining
if batch:
    cursor.executemany(INSERT_SQL, batch)
    db.commit()
    print(f"Committed final {len(batch)} rows (total {msg_count})")

print(f"No new messages for 5 seconds — consumer stopped.")
print(f"Total messages processed: {msg_count}")
print(f"Total runtime: {time.time() - pipeline_start:.2f} seconds")

cursor.close()
db.close()

