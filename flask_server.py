"""
Flask Server - Daily BigQuery to PostgreSQL ETL
Runs ETL process daily at scheduled time and provides API endpoints
"""
import os
from datetime import datetime, timedelta
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from google.cloud import bigquery
from google.oauth2 import service_account
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Flask app
app = Flask(__name__)

# ETL status
etl_status = {
    'last_run': None,
    'last_result': None,
    'last_processed_timestamp': None,
    'is_running': False
}

# Events to track
EVENTS = [
    'select_menu_category',
    'open_item_details',
    'select_commerce_category',
    'select_vendor',
    'add_item_to_favorites',
    'view_item'
]

# Timestamp file for state persistence (configurable via environment variable)
TIMESTAMP_FILE = os.getenv('TIMESTAMP_FILE', 'last_timestamp.txt')


def read_last_timestamp():
    """Read the last processed timestamp from file"""
    try:
        if os.path.exists(TIMESTAMP_FILE):
            with open(TIMESTAMP_FILE, 'r') as f:
                timestamp = int(f.read().strip())
                logger.info(f"Loaded last processed timestamp: {timestamp}")
                return timestamp
    except Exception as e:
        logger.warning(f"Error reading timestamp file: {e}")

    # If no timestamp exists, look back N hours from now
    lookback_hours = int(os.getenv('ETL_LOOKBACK_HOURS', '24'))
    fallback_time = datetime.now() - timedelta(hours=lookback_hours)
    fallback_timestamp = int(fallback_time.timestamp() * 1000000)  # Convert to microseconds
    logger.info(f"No timestamp file found, using fallback: {lookback_hours} hours ago ({fallback_timestamp})")
    return fallback_timestamp


def write_last_timestamp(timestamp):
    """Write the last processed timestamp to file"""
    try:
        with open(TIMESTAMP_FILE, 'w') as f:
            f.write(str(timestamp))
        logger.info(f"Updated last processed timestamp: {timestamp}")
    except Exception as e:
        logger.error(f"Error writing timestamp file: {e}")
        raise


def run_etl():
    """Run ETL process from BigQuery to PostgreSQL using timestamp-based incremental processing"""
    global etl_status

    if etl_status['is_running']:
        logger.warning("ETL already running, skipping")
        return

    etl_status['is_running'] = True
    start_time = datetime.now()

    try:
        # Get last processed timestamp
        last_timestamp = read_last_timestamp()
        last_timestamp_dt = datetime.fromtimestamp(last_timestamp / 1000000)
        logger.info(f"Starting incremental ETL from timestamp: {last_timestamp} ({last_timestamp_dt})")

        # Connect to BigQuery
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv('BQ_CREDENTIALS_PATH'),
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        bq_client = bigquery.Client(
            credentials=credentials,
            project=os.getenv('BQ_PROJECT_ID'),
            location=os.getenv('BQ_LOCATION')
        )

        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=os.getenv('PG_HOST'),
            port=os.getenv('PG_PORT'),
            database=os.getenv('PG_DATABASE'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD')
        )
        pg_table = os.getenv('PG_TABLE')

        # Create table if not exists
        cursor = pg_conn.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {pg_table} (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255),
            event_date DATE,
            event_timestamp BIGINT,
            event_name VARCHAR(255),
            event_id VARCHAR(255),
            event_name_detail TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, event_timestamp, event_name)
        );
        CREATE INDEX IF NOT EXISTS idx_user_id ON {pg_table}(user_id);
        CREATE INDEX IF NOT EXISTS idx_event_date ON {pg_table}(event_date);
        CREATE INDEX IF NOT EXISTS idx_event_name ON {pg_table}(event_name);
        CREATE INDEX IF NOT EXISTS idx_event_timestamp ON {pg_table}(event_timestamp);
        """
        cursor.execute(create_table_query)
        pg_conn.commit()

        # Build BigQuery query with timestamp filtering
        table_wildcard = f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_TABLE_PREFIX')}*"
        event_list = "', '".join(EVENTS)

        query = f"""
            SELECT user_id, event_name, event_timestamp, event_params, event_date
            FROM `{table_wildcard}`
            WHERE user_id IS NOT NULL AND user_id != ''
            AND event_name IN ('{event_list}')
            AND event_timestamp > {last_timestamp}
            ORDER BY event_timestamp ASC
        """

        logger.info(f"Querying BigQuery with wildcard table: {table_wildcard}")
        rows = list(bq_client.query(query).result())
        logger.info(f"Found {len(rows)} new rows since last timestamp")

        # Process rows and track max timestamp
        all_data = []
        max_timestamp = last_timestamp

        for row in rows:
            event_id = None
            event_name_detail = None

            if row.event_params:
                for param in row.event_params:
                    if param.get('key') == 'id':
                        event_id = param.get('value', {}).get('string_value')
                    if param.get('key') == 'name':
                        event_name_detail = param.get('value', {}).get('string_value')

            all_data.append((
                row.user_id,
                row.event_date,
                row.event_timestamp,
                row.event_name,
                event_id,
                event_name_detail
            ))

            # Track maximum timestamp
            if row.event_timestamp > max_timestamp:
                max_timestamp = row.event_timestamp

        # Load to PostgreSQL (append, don't remove old data)
        if all_data:
            insert_query = f"""
                INSERT INTO {pg_table}
                (user_id, event_date, event_timestamp, event_name, event_id, event_name_detail)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, event_timestamp, event_name) DO NOTHING
            """
            execute_batch(cursor, insert_query, all_data, page_size=1000)
            pg_conn.commit()
            rows_loaded = cursor.rowcount
        else:
            rows_loaded = 0

        cursor.close()
        pg_conn.close()

        # Update timestamp file only after successful commit
        if max_timestamp > last_timestamp:
            write_last_timestamp(max_timestamp)
            etl_status['last_processed_timestamp'] = max_timestamp

        duration = (datetime.now() - start_time).total_seconds()
        result = {
            'status': 'success',
            'last_processed_timestamp': max_timestamp,
            'last_processed_time': datetime.fromtimestamp(max_timestamp / 1000000).isoformat(),
            'records_fetched': len(all_data),
            'records_inserted': rows_loaded,
            'duration_seconds': round(duration, 2)
        }

        logger.info(f"ETL completed: {result}")
        etl_status['last_run'] = datetime.now().isoformat()
        etl_status['last_result'] = result

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        etl_status['last_result'] = {'status': 'failed', 'error': str(e)}
        etl_status['last_run'] = datetime.now().isoformat()

    finally:
        etl_status['is_running'] = False


# API Endpoints
@app.route('/')
def home():
    return jsonify({
        'service': 'BigQuery to PostgreSQL ETL (Timestamp-based Incremental)',
        'status': 'running',
        'endpoints': {
            '/health': 'Health check',
            '/status': 'ETL status and current timestamp',
            '/trigger': 'Manually trigger incremental ETL (POST)'
        }
    })


@app.route('/health')
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


@app.route('/status')
def status():
    # Read current timestamp from file
    try:
        current_timestamp = read_last_timestamp()
        current_timestamp_dt = datetime.fromtimestamp(current_timestamp / 1000000).isoformat()
    except:
        current_timestamp = None
        current_timestamp_dt = None

    return jsonify({
        **etl_status,
        'current_timestamp_file': current_timestamp,
        'current_timestamp_readable': current_timestamp_dt
    })


@app.route('/trigger', methods=['POST'])
def trigger():
    if etl_status['is_running']:
        return jsonify({'status': 'error', 'message': 'ETL already running'}), 409

    run_etl()
    return jsonify({'status': 'success', 'result': etl_status['last_result']})


if __name__ == '__main__':
    # Setup scheduler for daily runs
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=run_etl,
        trigger=CronTrigger(
            hour=int(os.getenv('ETL_SCHEDULE_HOUR', '2')),
            minute=int(os.getenv('ETL_SCHEDULE_MINUTE', '0'))
        ),
        id='daily_etl',
        name='Daily ETL Job'
    )
    scheduler.start()
    logger.info("Scheduler started - ETL will run daily")

    # Start Flask server
    try:
        app.run(
            host='0.0.0.0',
            port=int(os.getenv('FLASK_PORT', '5000')),
            debug=False
        )
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped")
