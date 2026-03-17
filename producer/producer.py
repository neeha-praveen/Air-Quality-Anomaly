import requests
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

API_KEY = os.getenv("API_KEY")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "air_quality_raw"

cities = [
    {"city": "Bangalore",          "lat": 12.9716,   "lon": 77.5946},
    {"city": "Delhi",              "lat": 28.7041,   "lon": 77.1025},
    {"city": "Mumbai",             "lat": 19.0760,   "lon": 72.8777},
    {"city": "Chennai",            "lat": 13.0827,   "lon": 80.2707},
    {"city": "Hyderabad",          "lat": 17.3850,   "lon": 78.4867},
    {"city": "Thiruvananthapuram", "lat": 8.524139,  "lon": 76.936638}
]


def fetch_live_data(city_info):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={city_info['lat']}&lon={city_info['lon']}&appid={API_KEY}"
    response = requests.get(url)
    components = response.json()["list"][0]["components"]
    return {
        "city":      city_info["city"],
        "lat":       city_info["lat"],
        "lon":       city_info["lon"],
        "pm25":      components["pm2_5"],
        "pm10":      components["pm10"],
        "co":        components["co"],
        "no2":       components["no2"],
        "so2":       components["so2"],
        "o3":        components["o3"],
        "timestamp": datetime.utcnow().isoformat()
    }


# ── PHASE 1: Kaggle historical data (runs once) ───────────────────
if os.path.exists("historical_done.txt"):
    print("Phase 1 already done, skipping...\n")
else:
    print("Phase 1: Replaying historical data...")
    with open("producer/historical_data.json") as f:
        historical_records = json.load(f)

    for i, record in enumerate(historical_records):
        record["timestamp"] = datetime.utcnow().isoformat()
        producer.send(TOPIC, record)
        print(f"[HISTORICAL {i+1}/{len(historical_records)}] {record['city']} pm25={record['pm25']}")
        time.sleep(0.5)

    open("historical_done.txt", "w").close()
    print("Historical replay done!\n")


# ── PHASE 2: Live API ─────────────────────────────────────────────
print("Phase 2: Switching to live API...")
while True:
    for city in cities:
        record = fetch_live_data(city)
        producer.send(TOPIC, record)
        print(f"[LIVE] {record['city']} pm25={record['pm25']}")
    print("Waiting 5 minutes for next API update...\n")
    time.sleep(300)