# Shipping Data Product - Telegram Analytics Pipeline

A comprehensive data pipeline for collecting, transforming, and analyzing Telegram channel data using modern data engineering practices.

## 🏗️ Project Structure

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
DB_PASSWORD=password

# DBT Configuration
DBT_PROJECT_NAME=shipping_dbt_project
DBT_PROFILE_NAME=shipping_profile

# Telegram Configuration
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE=your_phone_number

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
 