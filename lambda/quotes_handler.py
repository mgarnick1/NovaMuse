import os
import random
import json
import boto3
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("QUOTES_TABLE")
table = dynamodb.Table(table_name)

GENRE_CACHE = None  # simple in-memory cache for Lambda container


def get_all_genres(table):
    global GENRE_CACHE
    if GENRE_CACHE:
        return GENRE_CACHE

    # Scan only the genre attribute
    response = table.scan(ProjectionExpression="genre")
    genres = list(
        {item["genre"] for item in response.get("Items", []) if "genre" in item}
    )

    GENRE_CACHE = genres
    return genres


def lambda_handler(event, context, test_genre=None):
    query_params = event.get("queryStringParameters") or {}
    author = query_params.get("author")
    genre = query_params.get("genre")

    # Search by author
    if author:
        response = table.query(
            IndexName="GSI2-Author",
            KeyConditionExpression=Key("GSI2PK").eq(f"AUTHOR#{author}"),
            Limit=20,
        )
        quotes = response.get("Items", [])
        return respond(quotes)

    # Search by genre
    if genre:
        response = table.query(
            IndexName="GSI1-Genre",
            KeyConditionExpression=Key("GSI1PK").eq(f"GENRE#{genre}"),
            Limit=20,
        )
        quotes = response.get("Items", [])
        return respond(quotes)

    # Random quote
    genres = get_all_genres(table)
    if not genres:
        return respond([])
    selected_genre = test_genre if test_genre else random.choice(genres)

    response = table.query(
        IndexName="GSI1-Genre",
        KeyConditionExpression=Key("GSI1PK").eq(f"GENRE#{selected_genre}"),
        Limit=20,
    )
    quotes = response.get("Items", [])
    if quotes:
        quote = random.choice(quotes)
        return respond([quote])
    else:
        return respond([])


def respond(items):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",  # use "*" only for dev
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(items),
    }
