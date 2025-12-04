#!/usr/bin/env bash
# ======== ALL-IN-ONE LAUNCH SCRIPT (NO-PERMISSION SAFE) ========

set -euo pipefail

# ---------- config (allow env override) ----------
TOPIC="${KAFKA_TOPIC:-traffic_raw}"
API_PORT="${API_PORT:-5001}"
DASH_PORT="${DASH_PORT:-8080}"

echo "🐳 bringing up Kafka + ZooKeeper..."
docker compose down -v >/dev/null 2>&1 || true
cat > docker-compose.yml <<'YML'
version: "3.9"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports: ["2181:2181"]

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: "zookeeper:2181"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
YML

docker compose up -d

# ---------- wait for kafka ----------
echo "⏳ waiting for Kafka..."
sleep 10

# ---------- create topic (idempotent) ----------
CID="$(docker compose ps -q kafka)"
docker exec -i "$CID" bash -lc \
  "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic '$TOPIC' --partitions 1 --replication-factor 1" || true
docker exec -i "$CID" bash -lc "kafka-topics --bootstrap-server localhost:9092 --list"

# ---------- python env setup (NO PERMISSION MODE) ----------
echo "🐍 setting up venv + packages (no permission mode)..."
rm -rf .venv || true
python -m venv .venv || python3 -m venv .venv

# shellcheck disable=SC1091
source .venv/Scripts/activate 2>/dev/null || source .venv/bin/activate || true

echo "⚙️ Installing dependencies..."
python -m pip install -U pip || python.exe -m pip install -U pip || true
python -m pip install -r requirements.txt --no-warn-script-location || \
python.exe -m pip install --user -r requirements.txt --no-warn-script-location

mkdir -p logs

# ---------- run components ----------
echo "🚗 starting producer..."
nohup python src/producer.py > logs/producer.log 2>&1 &

echo "🧾 starting consumer..."
nohup python src/consumer_mysql.py > logs/consumer.log 2>&1 &

echo "🛰️  starting analytics API..."
nohup python charts/analytics_api.py > logs/api.log 2>&1 &
sleep 5

echo "🖥️  starting dashboard server..."
nohup python -m http.server "$DASH_PORT" -d charts > logs/ui.log 2>&1 &
sleep 5

# ---------- open browser automatically ----------
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "http://localhost:$DASH_PORT" >/dev/null 2>&1 &
elif command -v start >/dev/null 2>&1; then
  start "http://localhost:$DASH_PORT" >/dev/null 2>&1 &
elif command -v open >/dev/null 2>&1; then
  open "http://localhost:$DASH_PORT" >/dev/null 2>&1 &
fi

# ---------- links ----------
echo ""
echo "✅ ALL SYSTEMS GO!"
echo "Dashboard →  http://localhost:$DASH_PORT"
echo "API Health → http://localhost:$API_PORT/api/health"
echo "API KPI →    http://localhost:$API_PORT/api/kpi"
echo ""
echo "Logs in ./logs/"
echo "Stop everything with:  docker compose down && pkill -f 'producer.py|consumer_mysql.py|analytics_api.py|http.server'"
