import os
import logging
import asyncio
import polars as pl
from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker
from pydantic import BaseModel

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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

async def analyze_delay_batch(messages: list[Delivery], logger: Logger):
    """
    Optimized batch processor using Polars for vectorized datetime math.
    """
    if not messages:
        return

    try:
        # Convert Pydantic models to dicts for Polars
        raw_data = [m.model_dump() for m in messages]
        df = pl.from_dicts(raw_data)
        
        # Deduplicate and Clean
        # Unique check prevents double-counting if a producer retries
        df = df.unique(subset=["delivery_id"])
        
        # Vectorized DateTime Conversion & Math
        # Using .cast(pl.Datetime) if strings are ISO8601, 
        # or .str.to_datetime if they are custom formats.
        df = df.with_columns([
        pl.col("expected_time").cast(pl.Datetime),
        pl.col("delivery_time").cast(pl.Datetime)
        ]).drop_nulls(subset=["expected_time", "delivery_time"])

        if not df.is_empty():
            # Calculate Delay: (Delivery - Expected)
            df = df.with_columns(
                ((pl.col("delivery_time") - pl.col("expected_time"))
                .dt.total_minutes())
                .alias("delay_min")
            )
            
            # 4. Analytics
            delayed_df = df.filter(pl.col("delay_min") > 0)
            avg_delay = df["delay_min"].mean() or 0
            
            logger.info(
                f"Processed {len(df)} records | "
                f"Delayed: {len(delayed_df)} | "
                f"Avg Delay: {avg_delay:.2f} min"
            )

    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        # FastStream handles the nack/retry logic if you re-raise
        raise e

if __name__ == "__main__":
    asyncio.run(app.run())