import boto3
import json
from datetime import datetime, timedelta
from pyowm import OWM
import os
import time

def get_berlin_time():
    # Get the current UTC time
    utc_time = datetime.utcnow()

    # Calculate the Berlin offset dynamically
    # Berlin is UTC+1, but during DST, it becomes UTC+2
    is_dst = time.localtime().tm_isdst
    berlin_offset = timedelta(hours=1 if not is_dst else 2)

    # Adjust the time to Berlin local time
    berlin_time = utc_time + berlin_offset

    return berlin_time.isoformat()

def lambda_handler(event, context):
    # TODO implement 

    OWM_API_KEY = os.environ["owm_api_key"] 
    KINESIS_STREAM_NAME = os.environ["kinesis_stream_name"]
    REGION = os.environ["region"]


    owm = OWM(OWM_API_KEY)
    mgr = owm.weather_manager()
    kinesis_client = boto3.client("kinesis", region_name=REGION)

    # get weather at Berlin with API
    observation = mgr.weather_at_place('Berlin,DE')
    weather = observation.weather

    # get weather details
    weather_data = {
        "city": "Berlin",
        "temperature": weather.temperature("celsius"),
        "humidity": weather.humidity,
        "pressure": weather.pressure,
        "wind": weather.wind(),
        "clouds": weather.clouds,
        "status": weather.status,
        "detailed_status": weather.detailed_status,
        "sunrise": datetime.utcfromtimestamp(weather.sunrise_time()).isoformat(),
        "sunset": datetime.utcfromtimestamp(weather.sunset_time()).isoformat(),
        "ref_time": datetime.utcfromtimestamp(weather.ref_time).isoformat(),  # Convert UNIX timestamp
        "timestamp": get_berlin_time(),  # Add collection time in UTC
    }

    # convert data to JSON string
    weather_data_json = json.dumps(weather_data)

    # send data to Kinesis stream
    response = kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=weather_data_json,
        PartitionKey="partitionKey"
    )
    print("Data sent to Kinesis:", json.dumps(weather_data))

    print(f"Data sent to Kinesis: {response}")
    return {
        "statusCode": 200,
        "body": json.dumps("Weather data collected and sent to Kinesis")
    }