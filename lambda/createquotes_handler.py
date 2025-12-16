from datetime import datetime
import hashlib
import os
import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ.get("QUOTES_TABLE"))


def lambda_handler(event, context, test_genre=None):
    body = json.loads(event.get("body") or "{}")
    text = body.get("text")
    author = body.get("author")
    genre = body.get("genre")
    source = body.get("source")

    if not text or not author or not genre or not source:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "text, author, and genre are required"}),
        }
    
    created_at = datetime.utcnow().isoformat() + "Z"
    quote_id = hashlib.md5(text.encode("utf-8")).hexdigest()[:8]
    item = {
        "PK": f"QUOTE#{quote_id}",
        "SK": "METADATA",
        "quoteId": quote_id,
        "GSI1PK": f"GENRE#{genre}",
        "GSI1SK": f"CREATED#{created_at}",
        "GSI2PK": f"AUTHOR#{author}",
        "GSI2SK": f"CREATED#{created_at}",
        "text": text,
        "author": author,
        "genre": genre,
        "source": source,
        "createdAt": created_at,
    }

    try:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                "statusCode": 409,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Quote already exists"}),
            }
        else:
            raise

    return {
        "statusCode": 201,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"message": "Quote created successfully"}),
    }
