import base64
import json
import boto3
import os
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("QUOTES_TABLE")
table = dynamodb.Table(table_name)


def encode_cursor(key):
    if not key:
        return None
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()


def decode_cursor(cursor):
    if not cursor:
        return None
    return json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())


def lambda_handler(event, context, test_genre=None):
    query_params = event.get("queryStringParameters") or {}
    limit = min(int(query_params.get("limit", "10")), 50)
    cursor = query_params.get("cursor")
    genre = query_params.get("genre")

    exclusive_start_key = decode_cursor(cursor) if cursor else None
    try:
        if genre:
            kwargs = {
                "IndexName": "GSI1-Genre",
                "KeyConditionExpression": Key("GSI1PK").eq(f"GENRE#{genre}"),
                "Limit": limit,
                "ScanIndexForward": True,
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            response = table.query(**kwargs)
        else:
            # Full table browse (allowed, but less efficient)
            kwargs = {"Limit": limit}
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key
            response = table.scan(**kwargs)
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",  # use "*" only for dev
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            },
            "body": json.dumps({"error": str(e)}),
        }
    items = response.get("Items", [])
    next_cursor = encode_cursor(response.get("LastEvaluatedKey"))

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # use "*" only for dev
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps({"items": items, "nextCursor": next_cursor}),
    }
