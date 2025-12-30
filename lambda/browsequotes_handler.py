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
    author = query_params.get("author")

    exclusive_start_key = decode_cursor(cursor) if cursor else None
    try:
        if genre:
            kwargs = {
                "IndexName": "GSI1-Genre",
                "KeyConditionExpression": Key("GSI1PK").eq(f"GENRE#{genre}"),
                "Limit": limit,
                "ScanIndexForward": False,
                "ProjectionExpression": "quoteId, #t, author, genre, #s, createdAt",
                "ExpressionAttributeNames": {"#t": "text", "#s": "source"},
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            response = table.query(**kwargs)

            # If author is also provided, filter in memory
            if author:
                original_count = response.get("Count", len(response.get("Items", [])))
                filtered_items = [
                    item for item in response["Items"] if item.get("author") == author
                ]

                response["Items"] = filtered_items
                response["Count"] = len(filtered_items)

                # Pagination handling when filtering:
                # - If we got a full page after filtering → keep LastEvaluatedKey (best effort)
                # - If filtering reduced the page size → clear LastEvaluatedKey to avoid wrong skips/duplicates
                if len(filtered_items) < original_count:  # Some items were filtered out
                    response.pop(
                        "LastEvaluatedKey", None
                    )  # Safer: no next page until refetch
                # Else: no filtering happened → safe to keep LastEvaluatedKey
        elif author:
            kwargs = {
                "IndexName": "GSI2-Author",
                "KeyConditionExpression": Key("GSI2PK").eq(f"AUTHOR#{author}"),
                "Limit": limit,
                "ScanIndexForward": False,
                "ProjectionExpression": "quoteId, #t, author, genre, #s, createdAt",
                "ExpressionAttributeNames": {"#t": "text", "#s": "source"},
            }
            if exclusive_start_key:
                kwargs["ExclusiveStartKey"] = exclusive_start_key

            response = table.query(**kwargs)
        else:
            # Full table browse (allowed, but less efficient)
            kwargs = {
                "Limit": limit,
                "ProjectionExpression": "quoteId, #t, author, genre, #s, createdAt",
                "ExpressionAttributeNames": {"#t": "text", "#s": "source"},
            }
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
