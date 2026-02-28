import json
import urllib.request
import boto3
import os
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

def fetch_all_capitals():
    url = "https://restcountries.com/v3.1/all?fields=name,capital,capitalInfo"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    response = urllib.request.urlopen(req)
    countries = json.loads(response.read().decode("utf-8"))

    cities = []
    for country in countries:
        try:
            capital = country["capital"][0]
            lat = country["capitalInfo"]["latlng"][0]
            lon = country["capitalInfo"]["latlng"][1]
            cities.append({
                "name": capital,
                "country": country["name"]["common"],
                "latitude": lat,
                "longitude": lon
            })
        except (KeyError, IndexError):
            continue
    return cities

def fetch_weather(city):
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={city['latitude']}&longitude={city['longitude']}"
        f"&current=temperature_2m,windspeed_10m,weathercode,relative_humidity_2m"
        f"&timezone=auto"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    response = urllib.request.urlopen(req, timeout=10)
    weather = json.loads(response.read().decode("utf-8"))

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "city": city["name"],
        "country": city["country"],
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "temperature_c": weather["current"]["temperature_2m"],
        "windspeed_kmh": weather["current"]["windspeed_10m"],
        "humidity": weather["current"]["relative_humidity_2m"],
        "weather_code": weather["current"]["weathercode"]
    }

def lambda_handler(event, context):
    cities = fetch_all_capitals()
    print(f"Total cities: {len(cities)}")

    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_weather, city): city for city in cities}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Skipping {futures[future]['name']}: {str(e)}")
                continue

    # Convert to Parquet
    df = pd.DataFrame(results)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Save to S3
    s3 = boto3.client("s3")
    bucket = os.environ["BUCKET_NAME"]
    date_prefix = datetime.now().strftime("%Y%m%d%H%M%S")

    s3.put_object(
        Bucket=bucket,
        Key=f"api-data/weather-{date_prefix}.parquet",
        Body=buffer.getvalue()
    )

    print(f"Saved {len(results)} records to S3 as Parquet")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "API weather data saved as Parquet",
            "total_cities": len(results),
            "s3_key": f"api-data/weather-{date_prefix}.parquet"
        })
    }