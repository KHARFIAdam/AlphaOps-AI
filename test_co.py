import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

dsn = f"dbname={os.getenv('PGDATABASE')} user={os.getenv('PGUSER')} password={os.getenv('PGPASSWORD')} host={os.getenv('PGHOST')} port={os.getenv('PGPORT')}"
conn = psycopg2.connect(dsn)
cur = conn.cursor()
cur.execute("SELECT current_database(), current_user, current_schema();")
print("Connected as:", cur.fetchone())
print("OK !")
conn.close()