import os
import polars as pl
from faststream import FastStream
from faststream.kafka import KafkaBroker
from pydantic import BaseModel

# Get Kafka URL from environment or default to localhost for 'uv run'
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")

broker = KafkaBroker(KAFKA_URL)
app = FastStream(broker)

class Delivery(BaseModel):
    delivery_id: int
    city: str
    pickup_time: str
    expected_time: str
    delivery_time: str

@broker.subscriber("deliveries", batch=True)
async def analyze_delay(messages: list[Delivery]):
    # Convert batch to Polars for vectorized processing
    df = pl.from_dicts([m.model_dump() for m in messages])
    
    # Fast Polars datetime conversion and delay logic
    df = df.with_columns([
        pl.col("expected_time").str.to_datetime(),
        pl.col("delivery_time").str.to_datetime()
    ]).with_columns(
        (pl.col("delivery_time") - pl.col("expected_time")).dt.total_minutes().alias("delay_min")
    )
    
    delayed = df.filter(pl.col("delay_min") > 0)
    print(f"Processed batch: {len(df)} rows. Delays detected: {len(delayed)}")