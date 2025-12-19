import sys
import os
from unittest.mock import patch
import pytest
from moto import mock_dynamodb
import boto3
import json
from botocore.exceptions import ClientError


sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../lambda"))
)

# MUST be set before importing lambda
os.environ["QUOTES_TABLE"] = "NovaMuseQuotes"

from createquotes_handler import lambda_handler

TABLE_NAME = "NovaMuseQuotes"

ADMIN_CLAIMS = {
    "cognito:groups": "admins",
    "email": "admin@example.com",
    "sub": "test-user-id",
}

NON_ADMIN_CLAIMS = {
    "cognito:groups": "",
    "email": "user@example.com",
    "sub": "test-user-id",
}


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
        "requestContext": {
            "authorizer": {"claims": ADMIN_CLAIMS}  # or NON_ADMIN_CLAIMS
        },
        "body": json.dumps(
            {
                "text": "Do or do not. There is no try.",
                "author": "Yoda",
                "genre": "sci-fi",
                "source": "Star Wars",
            }
        ),
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
    event = {
        "requestContext": {
            "authorizer": {"claims": ADMIN_CLAIMS}  # or NON_ADMIN_CLAIMS
        },
        "body": json.dumps({"text": "Incomplete quote"}),
    }

    response = lambda_handler(event, None)

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "required" in body["error"]


def test_create_quote_duplicate(dynamodb_table):
    text = "Fear is the mind-killer."
    event = {
        "requestContext": {
            "authorizer": {"claims": ADMIN_CLAIMS}  # or NON_ADMIN_CLAIMS
        },
        "body": json.dumps(
            {
                "text": text,
                "author": "Paul Atreides",
                "genre": "sci-fi",
                "source": "Dune",
            }
        ),
    }

    # First insert
    response1 = lambda_handler(event, None)
    assert response1["statusCode"] == 201

    original_put_item = dynamodb_table.put_item

    def fake_put_item(*args, **kwargs):
        if "ConditionExpression" in kwargs:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
            )
        return original_put_item(*args, **kwargs)

    dynamodb_table.put_item = fake_put_item

    # Second insert (simulate duplicate)
    response2 = lambda_handler(event, None)
    assert response2["statusCode"] == 409
    body = json.loads(response2["body"])
    assert body["error"] == "Quote already exists"


def test_create_quote_as_non_admin(dynamodb_table):
    event = {
        "requestContext": {"authorizer": {"claims": NON_ADMIN_CLAIMS}},
        "body": json.dumps(
            {
                "text": "Test quote",
                "author": "Author",
                "genre": "sci-fi",
                "source": "Book",
            }
        ),
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 403
    body = json.loads(response["body"])
    assert "Admins only" in body["error"]


def test_create_quote_dynamodb_failure(dynamodb_table):
    event = {
        "requestContext": {"authorizer": {"claims": ADMIN_CLAIMS}},
        "body": json.dumps(
            {
                "text": "Test quote",
                "author": "Author",
                "genre": "sci-fi",
                "source": "Book",
            }
        ),
    }

    with patch("createquotes_handler.table.put_item") as mock_put:
        mock_put.side_effect = ClientError(
            error_response={"Error": {"Code": "SomeOtherError", "Message": "fail"}},
            operation_name="PutItem",
        )
        with pytest.raises(ClientError):
            lambda_handler(event, None)


def test_create_quote_fields_present(dynamodb_table):
    event = {
        "requestContext": {"authorizer": {"claims": ADMIN_CLAIMS}},
        "body": json.dumps(
            {
                "text": "New quote",
                "author": "Author",
                "genre": "sci-fi",
                "source": "Book",
            }
        ),
    }
    response = lambda_handler(event, None)
    items = dynamodb_table.scan()["Items"]
    item = items[0]
    assert "createdAt" in item
    assert "quoteId" in item
    assert len(item["quoteId"]) == 8


def test_create_quote_groups_as_list(dynamodb_table):
    event = {
        "requestContext": {"authorizer": {"claims": {"cognito:groups": ["admins"]}}},
        "body": json.dumps(
            {
                "text": "List groups test",
                "author": "Tester",
                "genre": "sci-fi",
                "source": "Book",
            }
        ),
    }

    response = lambda_handler(event, None)
    assert response["statusCode"] == 201


def test_create_quote_dynamodb_unexpected_error(dynamodb_table):
    event = {
        "requestContext": {"authorizer": {"claims": {"cognito:groups": "admins"}}},
        "body": json.dumps(
            {
                "text": "A new unexpected error quote",
                "author": "Tester",
                "genre": "sci-fi",
                "source": "Test Source",
            }
        ),
    }

    # Patch the table's put_item to raise a different ClientError
    with patch("createquotes_handler.table.put_item") as mock_put:
        mock_error = ClientError(
            error_response={
                "Error": {"Code": "ProvisionedThroughputExceededException"}
            },
            operation_name="PutItem",
        )
        mock_put.side_effect = mock_error

        with pytest.raises(ClientError) as exc_info:
            lambda_handler(event, None)

        assert "ProvisionedThroughputExceededException" in str(exc_info.value)
