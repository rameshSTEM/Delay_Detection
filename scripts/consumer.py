import os
import json
import logging
from pathlib import Path
from datetime import datetime
import polars as pl
from faststream import FastStream, Logger
from faststream.kafka import KafkaBroker

# --- Config ---
KAFKA_URL = os.getenv("KAFKA_URL", "kafka:9092")
TOPIC_NAME = "deliveries"
OUTPUT_DIR = Path("/app/output")

# Define our three sinks
ALL_DELAYS_FILE = OUTPUT_DIR / "all_delays.csv"
RIDER_REPORT_FILE = OUTPUT_DIR / "rider_performance.csv"
BATCH_STATS_FILE = OUTPUT_DIR / "batch_summary.csv"

broker = KafkaBroker(KAFKA_URL)
app = FastStream(broker)

def append_csv(df: pl.DataFrame, path: Path):
    """Helper to append Polars DataFrame to CSV with header logic."""
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with open(path, mode="a") as f:
        df.write_csv(f, include_header=not file_exists)

@broker.subscriber(
    TOPIC_NAME,
    group_id="delay-detection-service",
    batch=True,
    max_records=1000,
    max_poll_interval_ms=3000, 
    auto_offset_reset="earliest",
) 
async def analyze_delay_batch(messages: list[str], logger: Logger):
    if not messages:
        return

    try:
        # 1. Parse and Transform
        raw_dicts = [json.loads(m) for m in messages]
        df = pl.DataFrame(raw_dicts).unique(subset=["delivery_id"])
        
        df = df.with_columns([
            pl.col("expected_time").str.to_datetime(),
            pl.col("delivery_time").str.to_datetime()
        ])

        # 2. Calculate Delay
        df = df.with_columns(
            ((pl.col("delivery_time") - pl.col("expected_time"))
            .dt.total_minutes())
            .alias("delay_min")
        )

        # 3. Filter for actual delays
        delayed_df = df.filter(pl.col("delay_min") > 0)

        if not delayed_df.is_empty():
            # A. Save All Individual Delays
            append_csv(delayed_df, ALL_DELAYS_FILE)

            # B. Generate Rider-Specific Report (Avg delay per driver in this batch)
            rider_report = (
                delayed_df.group_by("driver_id")
                .agg([
                    pl.len().alias("total_delayed_trips"),
                    pl.col("delay_min").mean().round(2).alias("avg_delay_min"),
                    pl.col("delay_min").max().alias("max_single_delay")
                ])
            )
            append_csv(rider_report, RIDER_REPORT_FILE)

            # C. Save Batch Summary Stats
            batch_stats = pl.DataFrame({
                "timestamp": [datetime.now()],
                "batch_size": [len(df)],
                "delayed_count": [len(delayed_df)],
                "avg_batch_delay": [df["delay_min"].mean()]
            })
            append_csv(batch_stats, BATCH_STATS_FILE)

            logger.info(f"✅ Saved reports: {len(delayed_df)} delays, {len(rider_report)} riders affected.")

    except Exception as e:
        logger.error(f"❌ Batch processing failed: {e}")
        raise e