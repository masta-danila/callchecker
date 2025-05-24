import os
import psycopg2
from dotenv import load_dotenv

# Загрузка переменных из .env
load_dotenv()


def get_db_client():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )