import psycopg2
import dotenv
import os
dotenv.load_dotenv()







# Replace with your own values
conn = psycopg2.connect(
  host=os.getenv("DB_HOST"),
  port=os.getenv("DB_PORT"),
  database=os.getenv("DB_NAME"),
  user=os.getenv("DB_USER"),
  password=os.getenv("DB_PASSWORD")
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())

cur.close()
conn.close()
