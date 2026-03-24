import psycopg2
from datetime import datetime, timezone

DB_CONFIG = {
    "host"    : "host.docker.internal",
    "port"    : 5432,
    "database": "Airflow",
    "user"    : "postgres",
    "password": "admin123"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def unix_to_datetime(ts):
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc)
    except Exception:
        return None


def log_pipeline(dag_name, task_name, rows_inserted=0, rows_updated=0,
                 rows_rejected=0, status="success", duration=0, error=None):
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_logs
            (dag_name, task_name, run_date, rows_inserted, rows_updated,
             rows_rejected, status, duration_seconds, error_message)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s)
        """, (dag_name, task_name, rows_inserted, rows_updated,
              rows_rejected, status, duration, error))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Erreur log_pipeline : {e}")