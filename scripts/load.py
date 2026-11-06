
import os #module for interacting with the operating system
from pathlib import Path #module for handling file paths
from dotenv import load_dotenv #module for loading environment variables
import psycopg2 #module for connecting to PostgreSQL databases

# Find the project root and load .env
BASE_DIR = Path(__file__).resolve().parent.parent #refer project root
load_dotenv(BASE_DIR / ".env") # load environment variables from .env file

# Read database settings
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

try:
    conn = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        connect_timeout=5,
    )

    print("SUCCESS: PostgreSQL connection is working!")

    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user;")
        result = cur.fetchone()
        print("Database:", result[0])
        print("User:", result[1])

    conn.close()

except Exception as e:
    print("FAILED: Could not connect to PostgreSQL.")
    print("Error:", e)
