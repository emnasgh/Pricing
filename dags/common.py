import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host"    : "postgres",
    "port"    : 5432,
    "dbname"  : "retail_pricing",
    "user"    : "postgres",
    "password": "admin123"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_last_updated():
    conn   = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(api_updated) FROM prices_raw")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None

def unix_to_datetime(ts):
    if ts is None:
        return None
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None

def log_pipeline(dag_name, task_name, rows_added, rows_modified, rows_rejected, status, duration_seconds, error_message=None):
    conn   = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_logs (
            dag_name, task_name, run_date,
            rows_added, rows_modified, rows_rejected,
            status, duration_seconds, error_message, created_at
        )
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, NOW())
    """, (
        dag_name, task_name,
        rows_added, rows_modified, rows_rejected,
        status, duration_seconds, error_message
    ))
    conn.commit()
    cursor.close()
    conn.close()