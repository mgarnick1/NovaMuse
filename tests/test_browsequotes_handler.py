from datetime import datetime
import sys
import os
import json
import pytest
from moto import mock_dynamodb
import boto3
import base64

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../lambda"))
)

# MUST be set before importing lambda
os.environ["QUOTES_TABLE"] = "NovaMuseQuotes"

from browsequotes_handler import lambda_handler, encode_cursor, decode_cursor

TABLE_NAME = "NovaMuseQuotes"


@pytest.fixture
def dynamodb_table():
    with mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1-Genre",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Insert sample quotes
        for i in range(1, 21):
            now = datetime.utcnow().isoformat() + "Z"
            table.put_item(
                Item={
                    "PK": f"QUOTE#{i}",
                    "SK": "METADATA",
                    "text": f"Quote number {i}",
                    "author": "Author A" if i % 2 == 0 else "Author B",
                    "genre": "sci-fi" if i <= 10 else "fantasy",
                    "GSI1PK": f"GENRE#{'sci-fi' if i <= 10 else 'fantasy'}",
                    "GSI1SK": f"CREATED#{now}",
                }
            )

        yield table


def test_first_page(dynamodb_table):
    event = {"queryStringParameters": {"limit": "5", "genre": "sci-fi"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert len(body["items"]) == 5
    assert body["items"][0]["text"] == "Quote number 1"


def test_second_page_with_cursor(dynamodb_table):
    # Get first page
    event1 = {"queryStringParameters": {"limit": "5", "genre": "sci-fi"}}
    response1 = lambda_handler(event1, None)
    body1 = json.loads(response1["body"])
    cursor = body1["nextCursor"] 

    # Get second page
    event2 = {
        "queryStringParameters": {"limit": "5", "genre": "sci-fi", "cursor": cursor}
    }
    response2 = lambda_handler(event2, None)
    body2 = json.loads(response2["body"])
    assert len(body2["items"]) == 5
    assert body2["items"][0]["text"] == "Quote number 6"


def test_limit_exceeds_remaining(dynamodb_table):
    event = {"queryStringParameters": {"limit": "15", "genre": "sci-fi"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert len(body["items"]) == 10  # only 10 sci-fi quotes exist


def test_no_genre_provided(dynamodb_table):
    event = {"queryStringParameters": {"limit": "5"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    # Returns first 5 items (all genres)
    assert len(body["items"]) == 5
