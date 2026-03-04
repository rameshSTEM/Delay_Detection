import asyncio
import polars as pl
from faststream import FastStream
from faststream.kafka import KafkaBroker
from pydantic import BaseModel


#setup faststream producer
Broker = KafkaBroker("localhost:9092")
app = FastStream(Broker)

# Define the data model for deliveries
class Delivery(BaseModel):
    delivery_id: int
    city: str
    pickup_time: str
    driver_id: int
    expected_time: str
    delivery_time: str

async def produce_deliveries():
    async with Broker:
        # 2. Read CSV with Polars (Multithreaded & Fast)
        # Using scan_csv for large files (500k rows)
        df = pl.scan_csv("data/deliveries_500k.csv").collect()

        print(f"Starting stream for {len(df)} records...")

        # 3. Stream rows asynchronously
        # .to_dicts() is faster than pandas .iterrows()
        for row in df.to_dicts():
            await Broker.publish(
                Delivery(**row),
                topic="deliveries"
            )
            print(f"Sent: {row['delivery_id']}")
            
            # non-blocking sleep to simulate real-time streaming
            await asyncio.sleep(0.01)

if __name__ == "__main__":
    try:
        asyncio.run(produce_deliveries())
    except KeyboardInterrupt:
        print("Producer stopped.")






