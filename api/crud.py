from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

# These will be implemented to query the data marts

SCHEMA = "dbt_dev"

def get_top_products(db: Session, limit: int = 10):
    """Return the most frequently mentioned products."""
    result = db.execute(
        text(f"""
        SELECT detected_object_class AS product_name, COUNT(*) AS mention_count
        FROM {SCHEMA}.fct_image_detections
        GROUP BY detected_object_class
        ORDER BY mention_count DESC
        LIMIT :limit
        """),
        {"limit": limit}
    )
    return [
        {"product_name": row[0], "mention_count": row[1]}
        for row in result.fetchall()
    ]


def get_channel_activity(db: Session, channel_name: str):
    """Return posting activity for a specific channel."""
    result = db.execute(
        text(f"""
        SELECT channel_name, DATE(message_timestamp) AS date, COUNT(*) AS message_count
        FROM {SCHEMA}.fct_messages
        WHERE channel_name = :channel_name
        GROUP BY channel_name, DATE(message_timestamp)
        ORDER BY date
        """),
        {"channel_name": channel_name}
    )
    return [
        {"channel_name": row[0], "date": str(row[1]), "message_count": row[2]}
        for row in result.fetchall()
    ]


def search_messages(db: Session, query: str):
    """Search for messages containing a specific keyword."""
    result = db.execute(
        text(f"""
        SELECT message_id, channel_name, message_text, message_timestamp
        FROM {SCHEMA}.fct_messages
        WHERE message_text ILIKE :pattern
        ORDER BY message_timestamp DESC
        LIMIT 50
        """),
        {"pattern": f"%{query}%"}
    )
    return [
        {
            "message_id": row[0],
            "channel_name": row[1],
            "message_text": row[2],
            "message_timestamp": str(row[3])
        }
        for row in result.fetchall()
    ] 