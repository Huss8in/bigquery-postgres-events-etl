import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Default events to track (matching flask_server.py)
DEFAULT_EVENTS = [
    'select_menu_category',
    'open_item_details',
    'select_commerce_category',
    'select_vendor',
    'add_item_to_favorites',
    'view_item'
]


class BigQueryExtractor:
    def __init__(self):
        # BigQuery Configuration
        self.bq_project_id = os.getenv('BQ_PROJECT_ID')
        self.bq_dataset = os.getenv('BQ_DATASET')
        self.bq_table_prefix = os.getenv('BQ_TABLE_PREFIX', 'events_')
        self.bq_location = os.getenv('BQ_LOCATION', 'US')
        self.bq_credentials_path = os.getenv('BQ_CREDENTIALS_PATH')

        # PostgreSQL Configuration
        self.pg_host = os.getenv('PG_HOST', 'localhost')
        self.pg_port = os.getenv('PG_PORT', '5432')
        self.pg_database = os.getenv('PG_DATABASE')
        self.pg_user = os.getenv('PG_USER')
        self.pg_password = os.getenv('PG_PASSWORD')
        self.pg_table = os.getenv('PG_TABLE', 'application_events')

        self.bq_client = None
        self.pg_conn = None

    def connect_bigquery(self):
        """Establish connection to BigQuery"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.bq_credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self.bq_client = bigquery.Client(
                credentials=credentials,
                project=self.bq_project_id,
                location=self.bq_location
            )
            logger.info("Successfully connected to BigQuery")
        except Exception as e:
            logger.error(f"Failed to connect to BigQuery: {e}")
            raise

    def connect_postgres(self):
        """Establish connection to PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_database,
                user=self.pg_user,
                password=self.pg_password
            )
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def build_query(self, date_from, date_to, events=None):
        """Build BigQuery SQL query with date range and event filters (using wildcard tables)"""

        # Build event filter
        event_filter = ""
        if events and len(events) > 0:
            event_list = "', '".join(events)
            event_filter = f"AND event_name IN ('{event_list}')"

        # Use wildcard table pattern (e.g., events_*)
        table_wildcard = f"{self.bq_project_id}.{self.bq_dataset}.{self.bq_table_prefix}*"

        # Convert dates to table suffixes (YYYYMMDD format)
        date_from_suffix = datetime.strptime(date_from, '%Y-%m-%d').strftime('%Y%m%d')
        date_to_suffix = datetime.strptime(date_to, '%Y-%m-%d').strftime('%Y%m%d')

        query = f"""
        SELECT
            user_id,
            event_name,
            event_timestamp,
            event_params,
            event_date
        FROM `{table_wildcard}`
        WHERE _TABLE_SUFFIX BETWEEN '{date_from_suffix}' AND '{date_to_suffix}'
            AND user_id IS NOT NULL
            AND user_id != ''
            {event_filter}
        ORDER BY event_timestamp DESC
        """

        return query

    def extract_data(self, date_from, date_to, events=None):
        """Extract data from BigQuery with filters"""
        try:
            query = self.build_query(date_from, date_to, events)

            logger.info(f"Extracting data from {date_from} to {date_to}")
            if events:
                logger.info(f"Filtering events: {', '.join(events)}")

            logger.info("Executing query...")
            logger.debug(f"Query: {query}")

            query_job = self.bq_client.query(query)
            results = list(query_job.result())

            logger.info(f"Extracted {len(results)} rows from BigQuery")

            return results

        except Exception as e:
            logger.error(f"Failed to extract data from BigQuery: {e}")
            raise

    def export_to_csv(self, data, output_file='export.csv'):
        """Export data to CSV file (matching flask_server.py schema)"""
        try:
            logger.info(f"Exporting data to {output_file}...")

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                # Define CSV headers matching PostgreSQL table structure
                fieldnames = ['user_id', 'event_date', 'event_timestamp', 'event_name', 'event_id', 'event_name_detail']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                row_count = 0
                for row in data:
                    # Extract event parameters (matching flask_server.py logic)
                    event_id = None
                    event_name_detail = None

                    if row.event_params:
                        for param in row.event_params:
                            if param.get('key') == 'id':
                                event_id = param.get('value', {}).get('string_value')
                            if param.get('key') == 'name':
                                event_name_detail = param.get('value', {}).get('string_value')

                    # Write row
                    writer.writerow({
                        'user_id': row.user_id,
                        'event_date': row.event_date,
                        'event_timestamp': row.event_timestamp,
                        'event_name': row.event_name,
                        'event_id': event_id,
                        'event_name_detail': event_name_detail
                    })
                    row_count += 1

                    if row_count % 1000 == 0:
                        logger.info(f"Exported {row_count} rows...")

                logger.info(f"Successfully exported {row_count} rows to {output_file}")
                return row_count

        except Exception as e:
            logger.error(f"Failed to export to CSV: {e}")
            raise

    def load_to_postgres(self, data, batch_size=1000):
        """Load data into PostgreSQL (matching flask_server.py schema)"""
        try:
            logger.info("Setting up PostgreSQL table...")

            # Create table if not exists (matching flask_server.py)
            cursor = self.pg_conn.cursor()
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.pg_table} (
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
            CREATE INDEX IF NOT EXISTS idx_user_id ON {self.pg_table}(user_id);
            CREATE INDEX IF NOT EXISTS idx_event_date ON {self.pg_table}(event_date);
            CREATE INDEX IF NOT EXISTS idx_event_name ON {self.pg_table}(event_name);
            CREATE INDEX IF NOT EXISTS idx_event_timestamp ON {self.pg_table}(event_timestamp);
            """
            cursor.execute(create_table_query)
            self.pg_conn.commit()

            logger.info("Loading data to PostgreSQL...")

            insert_query = f"""
                INSERT INTO {self.pg_table}
                (user_id, event_date, event_timestamp, event_name, event_id, event_name_detail)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, event_timestamp, event_name) DO NOTHING
            """

            batch = []
            row_count = 0

            for row in data:
                # Extract event parameters (matching flask_server.py logic)
                event_id = None
                event_name_detail = None

                if row.event_params:
                    for param in row.event_params:
                        if param.get('key') == 'id':
                            event_id = param.get('value', {}).get('string_value')
                        if param.get('key') == 'name':
                            event_name_detail = param.get('value', {}).get('string_value')

                batch.append((
                    row.user_id,
                    row.event_date,
                    row.event_timestamp,
                    row.event_name,
                    event_id,
                    event_name_detail
                ))

                if len(batch) >= batch_size:
                    execute_batch(cursor, insert_query, batch)
                    self.pg_conn.commit()
                    row_count += len(batch)
                    logger.info(f"Loaded {row_count} rows...")
                    batch = []

            # Load remaining rows
            if batch:
                execute_batch(cursor, insert_query, batch)
                self.pg_conn.commit()
                row_count += len(batch)

            cursor.close()
            logger.info(f"Successfully loaded {row_count} rows to PostgreSQL")
            return row_count

        except Exception as e:
            logger.error(f"Failed to load data to PostgreSQL: {e}")
            self.pg_conn.rollback()
            raise

    def close_connections(self):
        """Close database connections"""
        if self.pg_conn:
            self.pg_conn.close()
            logger.info("PostgreSQL connection closed")
        if self.bq_client:
            logger.info("BigQuery client closed")


def parse_date(date_string):
    """Parse date string in various formats"""
    formats = ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y']

    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue

    raise ValueError(f"Invalid date format: {date_string}. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Extract application events from BigQuery to CSV or PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract last 7 days to CSV (default events)
  python extract_bq.py --days 7 --output events.csv

  # Extract specific date range with default events
  python extract_bq.py --from 2026-01-01 --to 2026-01-10 --output jan_events.csv

  # Extract with specific events
  python extract_bq.py --from 2026-01-01 --to 2026-01-10 --events view_item add_to_cart --output events.csv

  # Load directly to PostgreSQL
  python extract_bq.py --from 2026-01-01 --to 2026-01-10 --postgres

  # Extract yesterday's data (default)
  python extract_bq.py --days 1 --output yesterday.csv
        """
    )

    # Date range options
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--from', dest='date_from', help='Start date (YYYY-MM-DD)')
    date_group.add_argument('--days', type=int, help='Extract last N days')

    parser.add_argument('--to', dest='date_to', help='End date (YYYY-MM-DD, default: today)')

    # Event filtering
    parser.add_argument('--events', nargs='+', help='Specific event names to extract (space-separated). Default: predefined events')

    # Output options
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('--output', '-o', help='Output CSV file path')
    output_group.add_argument('--postgres', action='store_true', help='Load directly to PostgreSQL')

    # Other options
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for PostgreSQL loading')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Calculate date range
    if args.days:
        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    else:
        date_from = parse_date(args.date_from)
        date_to = parse_date(args.date_to) if args.date_to else datetime.now().strftime('%Y-%m-%d')

    # Use default events if not specified
    events = args.events if args.events else DEFAULT_EVENTS

    # Default output
    if not args.output and not args.postgres:
        args.output = f"bq_export_{date_from}_to_{date_to}.csv"

    # Initialize extractor
    extractor = BigQueryExtractor()

    try:
        start_time = datetime.now()

        # Connect to BigQuery
        extractor.connect_bigquery()

        # Extract data
        data = extractor.extract_data(
            date_from=date_from,
            date_to=date_to,
            events=events
        )

        # Export or load data
        if args.postgres:
            extractor.connect_postgres()
            rows_processed = extractor.load_to_postgres(data, args.batch_size)
        else:
            rows_processed = extractor.export_to_csv(data, args.output)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"\n{'='*60}")
        logger.info(f"Extraction completed successfully!")
        logger.info(f"Date range: {date_from} to {date_to}")
        logger.info(f"Events: {', '.join(events)}")
        logger.info(f"Total rows: {rows_processed}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)
    finally:
        extractor.close_connections()


if __name__ == "__main__":
    main()