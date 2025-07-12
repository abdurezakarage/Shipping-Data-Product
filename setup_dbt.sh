#!/bin/bash

# Setup script for DBT project
echo "Setting up DBT project for Telegram Analytics..."

# Install DBT and dependencies
echo "Installing DBT and dependencies..."
pip install dbt-core==1.7.3 dbt-postgres==1.7.3 psycopg2-binary==2.9.9

# Create analytics schema in PostgreSQL
echo "Creating analytics schema..."
python -c "
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv('.env')

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'telegram_data'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

engine = create_engine(
    f\"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@\"
    f\"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}\"
)

with engine.connect() as conn:
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS analytics'))
    conn.commit()
    print('Analytics schema created successfully')
"

# Navigate to DBT project directory
cd dbt_telegram

# Test DBT connection
echo "Testing DBT connection..."
dbt debug

# Run DBT models
echo "Running DBT models..."
dbt run

# Run tests
echo "Running DBT tests..."
dbt test

# Generate documentation
echo "Generating DBT documentation..."
dbt docs generate

echo "DBT setup completed successfully!"
echo "You can view documentation by running: dbt docs serve" 