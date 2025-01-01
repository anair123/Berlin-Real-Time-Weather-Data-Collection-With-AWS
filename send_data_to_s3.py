import boto3
import csv
import os
import tempfile
from datetime import datetime

TABLE_NAME = os.environ["table_name"] 

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def flatten_weather_data(item):
    """Flatten a single weather data item."""

    data = {
        "City": item.get("city"),
        "Timestamp": item.get("timestamp"),
        "Temp": item.get("temperature", {}).get("temp"),
        "Temp Max": item.get("temperature", {}).get("temp_max"),
        "Temp Min": item.get("temperature", {}).get("temp_min"),
        "Feels Like": item.get("temperature", {}).get("feels_like"),
        "Humidity": item.get("humidity"),
        "Pressure": item.get("pressure", {}).get("press"),
        "Sea Level Pressure": item.get("pressure", {}).get("sea_level"),
        "Wind Speed": item.get("wind", {}).get("speed"),
        "Wind Deg": item.get("wind", {}).get("deg"),
        "Wind Gust": item.get("wind", {}).get("gust"),
        "Clouds": item.get("clouds"),
        "Status": item.get("status"),
        "Detailed Status": item.get("detailed_status"),
        "Sunrise": item.get("sunrise"),
        "Sunset": item.get("sunset"),
        "Ref Time": item.get("ref_time"),
    }
    return data

def fetch_data_from_dynamodb():
    response = table.scan()
    data = response.get("Items", [])

    # Handle pagination if necessary
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        data.extend(response.get("Items", []))
    
    return data

def save_to_csv(data):
    # add current name to the file name
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{current_date}.csv")
    file_path = temp_file.name

    # Get the headers from the first item's keys
    headers = list(data[0].keys())

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

    return file_path

def upload_to_s3(file_path, bucket_name, object_key):
    s3 = boto3.client("s3")
    with open(file_path, "rb") as file:
        s3.upload_fileobj(file, bucket_name, object_key)

def lambda_handler(event, context):
    try:
        # get data from dyanmodb
        raw_data = fetch_data_from_dynamodb()

        # flatten records
        flattened_data = [flatten_weather_data(item) for item in raw_data]

        # save data
        csv_file_path = save_to_csv(flattened_data)

        # add the current date 
        current_date = datetime.utcnow().strftime("%Y-%m-%d")
        bucket_name = os.environ["bucket_name"] 
        object_key = f"weather_data_{current_date}.csv"
        upload_to_s3(csv_file_path, bucket_name, object_key)

        # Clean up the temporary file
        os.remove(csv_file_path)

        return {
            "statusCode": 200,
            "body": f"Data successfully saved to S3 as {object_key} in bucket {bucket_name}."
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"An error occurred: {str(e)}"
        }