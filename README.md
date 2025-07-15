# Shipping Data Product - Telegram Analytics Pipeline

A comprehensive data pipeline for collecting, transforming, and analyzing Telegram channel data using modern data engineering practices.

##  Project Structure

```
Shipping-Data-Product/
├── data/                          # Data lake
│   ├── raw/                       # Raw JSON files
│   │   └── telegrammessages/      # Partitioned by date/channel
│   └── labeled/                   # Processed data
├── dbt_project/                   # DBT transformation project
│   ├── models/
│   │   ├── staging/              # Staging models
│   │   └── marts/                # Data marts (star schema)
│   ├── tests/                    # Custom data tests
│   ├── macros/                   # Reusable SQL macros
│   └── profiles.yml              # Database connections
├── src/                          # Python source code
│   ├── load_raw_data.py          # Data loader script
│   └── telegram_scrapper.py      # Telegram scraper
├── logs/                         # Application logs
├── Dockerfile                    # Container configuration
├── docker-compose.yml            # Multi-service setup
├── requirements.txt              # Python dependencies
├── .env                          # Environment variables
└── README.md                     # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Git
- Python 3.11+ (for local development)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd Shipping-Data-Product
```

### 2. Environment Configuration

Create a `.env` file with your configuration:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=#
DB_NAME=shipping_data_warehouse
DB_USER=postgres
DB_PASSWORD=#

# DBT Configuration
DBT_PROJECT_NAME=shipping_dbt_project
DBT_PROFILE_NAME=shipping_profile

# Telegram Configuration
TELEGRAM_API_ID=#
TELEGRAM_API_HASH=#
TELEGRAM_PHONE=#

### 3. Start the Environment

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 4. Load Raw Data

```bash
# Run the data loader
docker-compose exec data_pipeline python src/load_raw_data.py
```

### 5. Run DBT Transformations

```bash
# Access DBT container
docker-compose exec dbt bash

# Install DBT dependencies
dbt deps

# Run all models
dbt run


### Data Flow Diagram

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Telegram      │    │   Data Lake     │    │   PostgreSQL    │
│   Channels      │───▶│   (csv Files)  │───▶│   (Raw Schema)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Analytics     │◀───│   DBT Models    │◀───│   Staging       │
│   Dashboard     │    │   (Star Schema) │    │   Models        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_dates     │
                    │   (Date Key)    │
                    └─────────────────┘
                            │
                            │ date_key
                            ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   dim_channels  │    │   fct_messages  │    │   dim_senders   │
│   (Channel Key) │───▶│   (Fact Table)  │◀───│   (Sender Key)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        │ channel_key           │ message_key           │ sender_key
        ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Channel       │    │   Message       │    │   Sender        │
│   Analytics     │    │   Metrics       │    │   Analytics     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🔧 DBT Models

### Staging Models

- **`stg_telegram_messages`**: Cleaned and standardized raw data
  - Data type casting and validation
  - Date/time extraction and formatting
  - Calculated fields (message length, word count)
  - Message type classification

## Analytical API (FastAPI)

This project includes a FastAPI-based analytical API for querying business metrics from your data warehouse.

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run the API

From the project root:
```bash
uvicorn src.api.main:app --reload
```

- Visit [http://localhost:8000/docs](http://localhost:8000/docs) for interactive API documentation.
- Example endpoints:
  - `/api/reports/top-products?limit=10` — Most frequently mentioned products
  - `/api/channels/{channel_name}/activity` — Posting activity for a channel
  - `/api/search/messages?query=paracetamol` — Search messages by keyword


## ⚙️ Pipeline Orchestration (Dagster)

This project uses [Dagster](https://dagster.io/) for robust, observable, and schedulable data pipelines.

### Install Dagster

```bash
pip install dagster dagster-webserver
```

### Run the Dagster UI

From the project root:
```bash
export PYTHONPATH=.
# On Windows: set PYTHONPATH=.
dagster dev -f dagster_pipeline/repository.py
```

- Visit [http://localhost:3000](http://localhost:3000) to view, run, and monitor your pipeline.
- The pipeline includes these steps:
  1. `scrape_telegram_data` — Scrape Telegram messages
  2. `load_raw_to_postgres` — Load raw data into Postgres
  3. `run_dbt_transformations` — Run DBT models
  4. `run_yolo_enrichment` — Enrich data with YOLO object detection

 
