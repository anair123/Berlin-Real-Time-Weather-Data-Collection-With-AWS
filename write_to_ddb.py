import boto3
import json
import base64
from decimal import Decimal
import os

# Initialize DynamoDB
DYNAMODB_TABLE_NAME = os.environ["dynamo_db_table"]
REGION_NAME = os.environ["region"]

dynamodb = boto3.resource("dynamodb", region_name=REGION_NAME)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def lambda_handler(event, context):
    try:
        # Check if "Records" key exists in the event
        if "Records" not in event:
            raise KeyError("The 'Records' key is missing from the event payload.")

        # Process records from Kinesis
        for record in event["Records"]:
            # Check if "data" exists in the record
            if "kinesis" in record and "data" in record["kinesis"]:
                raw_data = record["kinesis"]["data"]
                print("Raw Kinesis data (Base64):", raw_data)

                if not raw_data.strip():
                    print("Empty data received, skipping...")
                    continue

                # Decode the Kinesis payload (Base64 -> JSON)
                try:
                    decoded_data = base64.b64decode(raw_data).decode('utf-8')
                    print("Decoded Kinesis data:", decoded_data)

                    # Parse the decoded JSON data
                    payload = json.loads(decoded_data, parse_float=Decimal)
                    print("Parsed JSON data:", payload)

                    # Ensure that the 'city' and 'timestamp' keys are in the payload
                    if "city" not in payload or "timestamp" not in payload:
                        raise KeyError("'city' or 'timestamp' key is missing in the payload.")

                    # Write data to DynamoDB
                    table.put_item(Item=payload)
                    print("Data written to DynamoDB:", payload)

                except (json.JSONDecodeError, KeyError, base64.binascii.Error) as e:
                    print(f"Error decoding data: {str(e)}")
                    continue
            else:
                print("Invalid record structure:", record)

        return {
            "statusCode": 200,
            "body": json.dumps("Data processed and stored successfully!")
        }

    except Exception as e:
        print("Error processing event:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps(f"An error occurred: {str(e)}")
        }
