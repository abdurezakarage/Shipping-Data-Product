#!/usr/bin/env python3
"""
Main Data Pipeline Script
Orchestrates the entire data loading and transformation process
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_command(command, description):
    """Run a shell command and log the result"""
    logger.info(f"Running: {description}")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True, 
            capture_output=True, 
            text=True
        )
        logger.info(f"✅ {description} completed successfully")
        if result.stdout:
            logger.debug(f"Output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False

def check_environment():
    """Check if required environment variables are set"""
    required_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.error("Please set these in your .env file")
        return False
    
    return True

def main():
    """Main pipeline execution"""
    logger.info("🚀 Starting Telegram Analytics Data Pipeline")
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Step 1: Load raw data into PostgreSQL
    logger.info("📊 Step 1: Loading raw data into PostgreSQL")
    if not run_command(
        "python src/data_loader.py",
        "Data loading from JSON files to PostgreSQL"
    ):
        logger.error("Data loading failed. Exiting.")
        sys.exit(1)
    
    # Step 2: Run DBT models
    logger.info("🔄 Step 2: Running DBT transformations")
    os.chdir('dbt_telegram')
    
    # Test DBT connection
    if not run_command("dbt debug", "DBT connection test"):
        logger.error("DBT connection failed. Exiting.")
        sys.exit(1)
    
    # Run DBT models
    if not run_command("dbt run", "DBT model execution"):
        logger.error("DBT model execution failed. Exiting.")
        sys.exit(1)
    
    # Run DBT tests
    if not run_command("dbt test", "DBT data quality tests"):
        logger.warning("Some DBT tests failed, but continuing...")
    
    # Generate documentation
    if not run_command("dbt docs generate", "DBT documentation generation"):
        logger.warning("Documentation generation failed, but continuing...")
    
    os.chdir('..')
    
    logger.info("✅ Data pipeline completed successfully!")
    logger.info("📈 Your analytics data is now ready for analysis")
    logger.info("📚 To view documentation, run: cd dbt_telegram && dbt docs serve")

if __name__ == "__main__":
    main() 