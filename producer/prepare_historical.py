import pandas as pd
import json

df = pd.read_csv("producer/india_city_aqi_2015_2023.csv")

# Keep only your 6 cities
cities_coords = {
    "Bangalore":          {"lat": 12.9716, "lon": 77.5946},
    "Delhi":              {"lat": 28.7041, "lon": 77.1025},
    "Mumbai":             {"lat": 19.0760, "lon": 72.8777},
    "Chennai":            {"lat": 13.0827, "lon": 80.2707},
    "Hyderabad":          {"lat": 17.3850, "lon": 78.4867},
    "Thiruvananthapuram": {"lat": 8.5241,  "lon": 76.9366}
}

df = df[df["city"].isin(cities_coords.keys())]

records = []
for _, row in df.iterrows():
    city = row["city"]
    records.append({
        "city":      city,
        "lat":       cities_coords[city]["lat"],
        "lon":       cities_coords[city]["lon"],
        "pm25":      float(row["pm25"]),
        "pm10":      float(row["pm10"]),
        "co":        float(row["co"]),
        "no2":       float(row["no2"]),
        "so2":       float(row["so2"]),
        "o3":        float(row["o3"]),
        "timestamp": row["date"]
    })

with open("producer/historical_data.json", "w") as f:
    json.dump(records, f)

print(f"Done! {len(records)} records saved to producer/historical_data.json")