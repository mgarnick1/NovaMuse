import sys
import os
import pytest
from moto import mock_dynamodb
import boto3
import json


sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../lambda"))
)

# MUST be set before importing lambda
os.environ["QUOTES_TABLE"] = "NovaMuseQuotes"

from createquotes_handler import lambda_handler

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
                {"AttributeName": "GSI2PK", "AttributeType": "S"},
                {"AttributeName": "GSI2SK", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1-Genre",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
                {
                    "IndexName": "GSI2-Author",
                    "KeySchema": [
                        {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        yield table


def test_create_quote_success(dynamodb_table):
    event = {
        "body": json.dumps(
            {
                "text": "Do or do not. There is no try.",
                "author": "Yoda",
                "genre": "sci-fi",
                "source": "Star Wars",
            }
        )
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == 201
    body = json.loads(response["body"])
    assert body["message"] == "Quote created successfully"

    # Verify item exists in DynamoDB
    items = dynamodb_table.scan()["Items"]
    assert len(items) == 1
    assert items[0]["author"] == "Yoda"


def test_create_quote_missing_fields(dynamodb_table):
    event = {"body": json.dumps({"text": "Incomplete quote"})}

    response = lambda_handler(event, None)

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "required" in body["error"]


def test_create_quote_duplicate(dynamodb_table):
    event = {
        "body": json.dumps(
            {
                "text": "Fear is the mind-killer.",
                "author": "Paul Atreides",
                "genre": "sci-fi",
                "source": "Dune",
            }
        )
    }

    # First insert
    response1 = lambda_handler(event, None)
    assert response1["statusCode"] == 201

    # Second insert (same text → same hash)
    response2 = lambda_handler(event, None)
    assert response2["statusCode"] == 409

    body = json.loads(response2["body"])
    assert body["error"] == "Quote already exists"
