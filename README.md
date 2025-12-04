# Kafkanem (your own)

Flow: producer → Kafka → consumer → MySQL → Flask API → Highcharts dashboard.

## Run
python -m pip install -r requirements.txt

# (optional) Kafka locally
docker compose up -d

# producer & consumer
python src/producer.py
python src/consumer_mysql.py

# API and dashboard
python charts/analytics_api.py
python -m http.server 8080 -d charts

Visit http://localhost:8080 (frontend), which calls http://localhost:5001 (API).
