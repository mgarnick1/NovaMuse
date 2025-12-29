import os
import sys
import json
import pytest
from moto import mock_dynamodb
import boto3

# Add lambda directory to path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../lambda"))
)

# MUST be set before importing lambda
os.environ["QUOTES_TABLE"] = "NovaMuseQuotes"

# Import the lambdas
from listgenres_handler import lambda_handler as genres_lambda
from listauthors_handler import lambda_handler as authors_lambda

TABLE_NAME = "NovaMuseQuotes"


@pytest.fixture
def dynamodb_table():
    with mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        # Only define the table's primary key attributes
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Insert sample data for testing
        table.put_item(
            Item={
                "PK": "QUOTE#1",
                "SK": "METADATA",
                "GSI1PK": "GENRE#fantasy",
                "GSI2PK": "AUTHOR#Terry Goodkind",
            }
        )
        table.put_item(
            Item={
                "PK": "QUOTE#2",
                "SK": "METADATA",
                "GSI1PK": "GENRE#sci-fi",
                "GSI2PK": "AUTHOR#Isaac Asimov",
            }
        )
        table.put_item(
            Item={
                "PK": "QUOTE#3",
                "SK": "METADATA",
                "GSI1PK": "GENRE#fantasy",
                "GSI2PK": "AUTHOR#Terry Goodkind",
            }
        )

        yield table


def test_list_genres(dynamodb_table, monkeypatch):
    monkeypatch.setenv("QUOTES_TABLE", TABLE_NAME)

    event = {"queryStringParameters": {}}
    response = genres_lambda(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert "fantasy" in body
    assert "sci-fi" in body
    assert len(body) == 2  # duplicates removed


def test_list_authors(dynamodb_table, monkeypatch):
    monkeypatch.setenv("QUOTES_TABLE", TABLE_NAME)

    event = {"queryStringParameters": {}}
    response = authors_lambda(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert "Terry Goodkind" in body
    assert "Isaac Asimov" in body
    assert len(body) == 2  # duplicates removed


def test_empty_table(monkeypatch):
    with mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        # Empty table (no items)
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        monkeypatch.setenv("QUOTES_TABLE", TABLE_NAME)

        # Test genres
        response = genres_lambda({}, None)
        body = json.loads(response["body"])
        assert body == []

        # Test authors
        response = authors_lambda({}, None)
        body = json.loads(response["body"])
        assert body == []
