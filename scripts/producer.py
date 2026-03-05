import asyncio
import os
import json
import polars as pl
import logging
from faststream import FastStream
from faststream.kafka import KafkaBroker
from pydantic import BaseModel

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer")

# --- Configuration ---
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9094")
TOPIC_NAME = "deliveries"
CSV_PATH = "data/deliveries_500k.csv"

broker = KafkaBroker(KAFKA_URL)
app = FastStream(broker)

class Delivery(BaseModel):
    delivery_id: int
    city: str
    pickup_time: str
    driver_id: int
    expected_time: str
    delivery_time: str

async def stream_data():
    async with broker:
        if not os.path.exists(CSV_PATH):
            logger.error(f"File not found: {CSV_PATH}")
            return

        logger.info(f"Reading {CSV_PATH}...")
        # Polars handles the 500k rows in milliseconds
        df = pl.scan_csv(CSV_PATH).collect()
        records = df.to_dicts()
        total_records = len(records)
        
        logger.info(f"Starting stream of {total_records} records to '{TOPIC_NAME}'")

        batch_size = 200 
        for i in range(0, total_records, batch_size):
            batch = records[i : i + batch_size]
            
            tasks = [
                broker.publish(
                    json.dumps(row),
                    topic=TOPIC_NAME,
                    key=str(row['delivery_id']).encode()
                )
                for row in batch
            ]
            
            await asyncio.gather(*tasks)

            if i % 10000 == 0:
                logger.info(f"Progress: {i}/{total_records} sent")

            await asyncio.sleep(0.01)

        logger.info("Streaming complete!")

if __name__ == "__main__":
    try:
        asyncio.run(stream_data())
    except KeyboardInterrupt:
        logger.warning("Producer stopped by user.")