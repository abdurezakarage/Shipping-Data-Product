import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv()  # Loads variables from a .env file if present

# --- CONFIGURATION ---
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME")

CSV_PATH = os.environ.get("CSV_PATH", "data/raw/image_detections.csv")

# --- CREATE DATABASE ENGINE ---
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# --- LOAD CSV AND WRITE TO DATABASE ---
df = pd.read_csv(CSV_PATH)
df.to_sql('image_detections', engine, schema='raw', if_exists='replace', index=False)

print(f"Loaded {len(df)} rows into raw.image_detections")
