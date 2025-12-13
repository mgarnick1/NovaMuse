import boto3
import uuid
import hashlib
from datetime import datetime

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table_name = "NovaMuseQuotes"
table = dynamodb.Table(table_name)

# Sample quotes
quotes = [
    {
        "text": "Do or do not. There is no try.",
        "author": "Yoda",
        "genre": "sci-fi",
        "source": "Star Wars: The Empire Strikes Back",
    },
    {
        "text": "Fear is the mind-killer.",
        "author": "Paul Atreides",
        "genre": "sci-fi",
        "source": "Dune Messiah",
    },
    {
        "text": "All we have to decide is what to do with the time that is given us.",
        "author": "Gandalf",
        "genre": "fantasy",
        "source": "The Lord of the Rings",
    },
    {
        "text": "It is our choices, Harry, that show what we truly are, far more than our abilities.",
        "author": "Dumbledore",
        "genre": "fantasy",
        "source": "Harry Potter and the Chamber of Secrets",
    },
    {
        "text": "I am Groot.",
        "author": "Groot",
        "genre": "sci-fi",
        "source": "Guardians of the Galaxy",
    },
    {
        "text": "Not all those who wander are lost.",
        "author": "Bilbo Baggins",
        "genre": "fantasy",
        "source": "The Hobbit",
    },
]

for q in quotes:
    quote_id = hashlib.md5(q["text"].encode("utf-8")).hexdigest()[:8]
    created_at = datetime.utcnow().isoformat() + "Z"

    existing = table.get_item(Key={"PK": f"QUOTE#{quote_id}", "SK": "METADATA"})
    if "Item" in existing:
        print(f"Quote already exists: {q['author']} - {q['text'][:30]}...")
        continue

    item = {
        "PK": f"QUOTE#{quote_id}",
        "SK": "METADATA",
        "quoteId": quote_id,
        "text": q["text"],
        "author": q["author"],
        "genre": q["genre"],
        "source": q["source"],
        "createdAt": created_at,
        # GSI1 - Genre
        "GSI1PK": f"GENRE#{q['genre']}",
        "GSI1SK": f"CREATED#{created_at}",
        # GSI2 - Author
        "GSI2PK": f"AUTHOR#{q['author']}",
        "GSI2SK": f"CREATED#{created_at}",
    }

    table.put_item(Item=item)
    print(f"Inserted quote {quote_id} by {q['author']}")
