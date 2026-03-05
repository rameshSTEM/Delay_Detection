import os
import logging
import asyncio
import polars as pl
from faststream import FastStream
from faststream.kafka import KafkaBroker
from pydantic import BaseModel

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("consumer")

KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9094")
TOPIC_NAME = "deliveries"

broker = KafkaBroker(KAFKA_URL)
app = FastStream(broker)

class Delivery(BaseModel):
    delivery_id: int
    city: str
    pickup_time: str
    driver_id: int
    expected_time: str
    delivery_time: str

@broker.subscriber(
    TOPIC_NAME,
    group_id="delay-detection-service",
    batch=True,
    max_records=1000,
    timeout_ms=3000,
    auto_offset_reset="earliest",
) # type: ignore
async def analyze_delay_batch(messages: list[dict]):
    if not messages:
        return

    try:
        # Convert to Polars
        df = pl.from_dicts(messages)
        
        # Deduplicate
        df = df.unique(subset=["delivery_id"])
        
        # Vectorized DateTime Conversion
        df = df.with_columns([
            pl.col("expected_time").str.to_datetime(strict=False),
            pl.col("delivery_time").str.to_datetime(strict=False)
        ]).drop_nulls(subset=["expected_time", "delivery_time"])

        if not df.is_empty():
            # Calculate Delay
            df = df.with_columns(
                (pl.col("delivery_time") - pl.col("expected_time"))
                .dt.total_minutes()
                .alias("delay_min")
            )
            
            delayed_count = df.filter(pl.col("delay_min") > 0).height
            logger.info(f"Batch: {len(df)} rows | Delays detected: {delayed_count}")

    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        raise e

if __name__ == "__main__":
    try:
        # Direct run call is more robust for standalone scripts
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("Consumer shutting down gracefully.")