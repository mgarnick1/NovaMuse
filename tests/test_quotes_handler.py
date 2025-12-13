import os
import pytest
import json
from moto import mock_dynamodb
import boto3
import sys
import random

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../lambda"))
)

# Set environment variable **before importing**
os.environ["QUOTES_TABLE"] = "NovaMuseQuotes"
# Import your lambda handler
from quotes_handler import lambda_handler

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

        # Insert sample quotes
        table.put_item(
            Item={
                "PK": "QUOTE#1",
                "SK": "METADATA",
                "text": "Do or do not. There is no try.",
                "author": "Yoda",
                "genre": "sci-fi",
                "source": "Star Wars: The Empire Strikes Back",
                "GSI1PK": "GENRE#sci-fi",
                "GSI1SK": "CREATED#1",
                "GSI2PK": "AUTHOR#Yoda",
                "GSI2SK": "CREATED#1",
            }
        )
        table.put_item(
            Item={
                "PK": "QUOTE#2",
                "SK": "METADATA",
                "text": "Fear is the mind-killer.",
                "author": "Paul Atreides",
                "genre": "sci-fi",
                "source": "Dune Messiah",
                "GSI1PK": "GENRE#sci-fi",
                "GSI1SK": "CREATED#2",
                "GSI2PK": "AUTHOR#Paul Atreides",
                "GSI2SK": "CREATED#2",
            }
        )

        # Set env var
        os.environ["QUOTES_TABLE"] = TABLE_NAME

        yield table


def test_query_by_author(dynamodb_table):
    event = {"queryStringParameters": {"author": "Yoda"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert len(body) == 1
    assert body[0]["author"] == "Yoda"


def test_query_by_genre(dynamodb_table):
    event = {"queryStringParameters": {"genre": "sci-fi"}}
    response = lambda_handler(event, None)
    body = json.loads(response["body"])
    assert len(body) == 2  # two quotes in sci-fi
    authors = [q["author"] for q in body]
    assert "Yoda" in authors
    assert "Paul Atreides" in authors


def test_random_quote(dynamodb_table):
    event = {"queryStringParameters": None}
    response = lambda_handler(event, None, test_genre="sci-fi")  # deterministic
    body = json.loads(response["body"])
    assert len(body) == 1
    assert "text" in body[0]
    assert "author" in body[0]
