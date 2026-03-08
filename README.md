**Real-Time Delivery Delay Detection Pipeline**

A high-performance data engineering pipeline designed to detect and analyze delivery delays in real-time. This system uses ***Kafka*** for event streaming, ***FastStream*** for asynchronous consumption, and ***Polars*** for vectorized batch processing.

**Overview**
This pipeline simulates a logistics environment where delivery events are streamed via Kafka. A consumer group balances the load to calculate delays (Actual vs. Expected time) and generates performance reports for riders and cities.

**Tech Stack**

    - Orchestration: Docker, Docker Compose

    - Message Broker: Apache Kafka (KRaft)

    - Processing Engine: Python 3.13, FastStream

    - Data Analysis: Polars (Vectorized DataFrame processing)

    - Dependency Management: uv 

## 📂 Project Structure

```text
.
├── data/                     #data files
│   ├── deliveries_500k.csv    
│   ├── traffic_data.csv       
│   └── weather_data.csv       
├── scripts/
│   ├── producer.py            
│   ├── consumer.py            
│   └── __init__.py
├── output/                    # auto created folder- Analytics results 
│   ├── all_delays.csv         
│   ├── batch_summary.csv      
│   └── rider_performance.csv  
├── docker-compose.yml         
├── Dockerfile                 
├── pyproject.toml             # Python dependencies
└── uv.lock                    # Dependency lockfile
```

**Project Execution**

1. *Start the infrastructure*

    docker compose up --build -d

2. *Execute the container and create a topic*
    
   docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic deliveries \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1


3. *Run the data producer*

    docker compose run data-producer

4. *Monitor logs* 

    docker compose logs -f delay-detector

**Output**
- The pipeline automatically generates ./output folder with 3 output .csv files


**Key Features**

    - Batch Processing: Configured to process 1,000 records or 3-second windows, optimizing I/O performance.

    - Vectorized Math: Uses Polars instead of Pandas to handle datetime calculations up to 10-100x faster.

    - Scalability: The delay-detector service is configured with replicas: 3, demonstrating Kafka consumer group load balancing.

    - Resilience: Uses Pydantic for data validation (optional) and robust error handling to prevent pipeline crashes on malformed JSON.

