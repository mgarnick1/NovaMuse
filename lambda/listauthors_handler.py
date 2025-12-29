import os
import json
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["QUOTES_TABLE"])


def lambda_handler(event, context):
    # Scan only the author attribute (GSI2PK)
    unique_authors = set()
    start_key = None

    while True:
        kwargs = {"ProjectionExpression": "GSI2PK"}
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key

        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            # Remove prefix "AUTHOR#" to get the actual author
            unique_authors.add(item["GSI2PK"].replace("AUTHOR#", ""))
        start_key = resp.get("LastEvaluatedKey")
        if not start_key:
            break

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # use "*" only for dev
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(sorted(unique_authors)),
    }
