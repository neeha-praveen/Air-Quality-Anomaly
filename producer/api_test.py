import requests
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

cities = [
    {"city": "Bangalore", "lat": 12.9716, "lon": 77.5946},
    {"city": "Delhi", "lat": 28.7041, "lon": 77.1025},
    {"city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"city": "Chennai", "lat": 13.0827, "lon": 80.2707},
    {"city": "Hyderabad", "lat": 17.3850, "lon": 78.4867},
    {"city": "Kolkata", "lat": 22.5726, "lon": 88.3639}
]

def fetch_city_data(city_info):

    lat = city_info["lat"]
    lon = city_info["lon"]
    city = city_info["city"]

    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"

    response = requests.get(url)
    data = response.json()

    components = data["list"][0]["components"]

    formatted = {
        "city": city,
        "lat": lat,
        "lon": lon,
        "pm25": components["pm2_5"],
        "pm10": components["pm10"],
        "co": components["co"],
        "no2": components["no2"],
        "so2": components["so2"],
        "o3": components["o3"],
        "timestamp": datetime.utcnow().isoformat()
    }

    return formatted


for city in cities:

    result = fetch_city_data(city)

    print(result)