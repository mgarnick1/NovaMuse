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


def test_cursor_none_returns_none():
    assert decode_cursor(None) is None
    assert encode_cursor(None) is None


def test_invalid_cursor_raises_exception(monkeypatch):
    # Patch table.query to raise an exception
    def fake_query(**kwargs):
        raise Exception("Dynamo error")

    monkeypatch.setattr("browsequotes_handler.table.query", fake_query)

    event = {"queryStringParameters": {"genre": "sci-fi"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert response["statusCode"] == 500
    assert "Dynamo error" in body["error"]


def test_limit_over_50_clamped(dynamodb_table):
    event = {"queryStringParameters": {"limit": "100", "genre": "sci-fi"}}

    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    # Should only return max 50 due to clamp
    assert len(body["items"]) <= 50


def test_full_scan_with_cursor(dynamodb_table):
    # Get a cursor for first page

    event1 = {"queryStringParameters": {"limit": "5"}}
    response1 = lambda_handler(event1, None)
    body1 = json.loads(response1["body"])
    cursor = body1["nextCursor"]

    # Use cursor for next page in scan mode (no genre)
    event2 = {"queryStringParameters": {"limit": "5", "cursor": cursor}}
    response2 = lambda_handler(event2, None)
    body2 = json.loads(response2["body"])
    assert len(body2["items"]) > 0


def test_encode_cursor_none():
    assert encode_cursor(None) is None


def test_decode_cursor_none():
    assert decode_cursor(None) is None


def test_encode_decode_cursor_roundtrip():
    key = {"PK": "QUOTE#1", "SK": "METADATA"}
    encoded = encode_cursor(key)
    decoded = decode_cursor(encoded)
    assert decoded == key


def test_limit_clamped_to_50(dynamodb_table):
    event = {"queryStringParameters": {"limit": "100"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert len(body["items"]) <= 50


def test_invalid_genre_returns_empty(dynamodb_table):
    event = {"queryStringParameters": {"genre": "nonexistent"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    # Should return empty items array
    assert body["items"] == []


def test_query_exception(monkeypatch):
    def fake_query(**kwargs):
        raise Exception("Dynamo error")

    monkeypatch.setattr("browsequotes_handler.table.query", fake_query)
    event = {"queryStringParameters": {"genre": "sci-fi"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert response["statusCode"] == 500
    assert "Dynamo error" in body["error"]


def test_scan_exception(monkeypatch):
    def fake_scan(**kwargs):
        raise Exception("Scan failed")

    monkeypatch.setattr("browsequotes_handler.table.scan", fake_scan)
    event = {"queryStringParameters": {}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert response["statusCode"] == 500
    assert "Scan failed" in body["error"]


def test_empty_genre_query(dynamodb_table):
    # Use a genre that exists but with cursor beyond data
    event = {"queryStringParameters": {"limit": "5", "genre": "sci-fi"}}
    # simulate start at last key
    last_key = {"PK": "QUOTE#999", "SK": "METADATA"}
    event["queryStringParameters"]["cursor"] = encode_cursor(last_key)
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert body["items"] == []
    assert body["nextCursor"] is None


def test_full_table_scan_no_cursor(dynamodb_table):
    event = {"queryStringParameters": {"limit": "5"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    # returns first 5 items from all genres
    assert len(body["items"]) == 5


def test_full_table_scan_with_cursor(dynamodb_table):
    # get first page
    event1 = {"queryStringParameters": {"limit": "5"}}
    response1 = lambda_handler(event1, None)
    body1 = json.loads(response1["body"])
    cursor = body1["nextCursor"]
    # get second page
    event2 = {"queryStringParameters": {"limit": "5", "cursor": cursor}}
    response2 = lambda_handler(event2, None)
    body2 = json.loads(response2["body"])
    assert len(body2["items"]) > 0
    assert body2["items"][0]["text"] != body1["items"][0]["text"]

def test_full_table_scan(dynamodb_table):
    # No author or genre → triggers scan branch
    event = {"queryStringParameters": {"limit": "5"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    
    assert len(body["items"]) <= 5
    assert all("text" in item for item in body["items"])

def test_limit_exceeds_total(dynamodb_table):
    event = {"queryStringParameters": {"genre": "fantasy", "limit": "50"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    # Only 10 fantasy quotes exist
    assert len(body["items"]) == 10

def test_full_table_scan_no_genre(dynamodb_table):
    event = {"queryStringParameters": {"limit": "5"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert len(body["items"]) == 5
    # Since no genre, items can be from either sci-fi or fantasy
    genres = set(item["genre"] for item in body["items"])
    assert genres.issubset({"sci-fi", "fantasy"})

def test_decode_none_cursor_returns_none():
    assert decode_cursor(None) is None


def test_encode_none_cursor_returns_none():
    assert encode_cursor(None) is None

