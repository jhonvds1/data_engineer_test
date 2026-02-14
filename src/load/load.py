import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)

print(conn.status)