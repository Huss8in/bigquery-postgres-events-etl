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


class BigQueryExtractor:
    def __init__(self):
        # BigQuery Configuration
        self.bq_project_id = os.getenv('BQ_PROJECT_ID')
        self.bq_dataset = os.getenv('BQ_DATASET')
        self.bq_table = os.getenv('BQ_TABLE')
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
            if self.bq_credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.bq_credentials_path
                )
                self.bq_client = bigquery.Client(
                    credentials=credentials,
                    project=self.bq_project_id
                )
            else:
                self.bq_client = bigquery.Client(project=self.bq_project_id)

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

    def build_query(self, date_from, date_to, events=None, additional_filters=None):
        """Build BigQuery SQL query with date range and event filters"""

        # Build event filter
        event_filter = ""
        if events and len(events) > 0:
            event_list = "', '".join(events)
            event_filter = f"AND event_name IN ('{event_list}')"

        # Build additional filters
        extra_filter = ""
        if additional_filters:
            extra_filter = f"AND {additional_filters}"

        query = f"""
        SELECT
            event_id,
            event_name,
            event_timestamp,
            user_id,
            session_id,
            TO_JSON_STRING(event_params) as event_data,
            TO_JSON_STRING(user_properties) as user_properties,
            TO_JSON_STRING(device) as device_info,
            geo.country as country,
            geo.city as city,
            platform,
            app_info.version as app_version
        FROM `{self.bq_project_id}.{self.bq_dataset}.{self.bq_table}`
        WHERE DATE(event_timestamp) BETWEEN '{date_from}' AND '{date_to}'
            {event_filter}
            {extra_filter}
        ORDER BY event_timestamp DESC
        """

        return query

    def extract_data(self, date_from, date_to, events=None, additional_filters=None):
        """Extract data from BigQuery with filters"""
        try:
            query = self.build_query(date_from, date_to, events, additional_filters)

            logger.info(f"Extracting data from {date_from} to {date_to}")
            if events:
                logger.info(f"Filtering events: {', '.join(events)}")

            logger.info("Executing query...")
            logger.debug(f"Query: {query}")

            query_job = self.bq_client.query(query)
            results = query_job.result()

            total_rows = results.total_rows
            logger.info(f"Extracted {total_rows} rows from BigQuery")

            return results, query

        except Exception as e:
            logger.error(f"Failed to extract data from BigQuery: {e}")
            raise

    def export_to_csv(self, data, output_file='export.csv'):
        """Export data to CSV file"""
        try:
            logger.info(f"Exporting data to {output_file}...")

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = None
                row_count = 0

                for row in data:
                    row_dict = dict(row)

                    if writer is None:
                        writer = csv.DictWriter(csvfile, fieldnames=row_dict.keys())
                        writer.writeheader()

                    writer.writerow(row_dict)
                    row_count += 1

                    if row_count % 1000 == 0:
                        logger.info(f"Exported {row_count} rows...")

                logger.info(f"Successfully exported {row_count} rows to {output_file}")
                return row_count

        except Exception as e:
            logger.error(f"Failed to export to CSV: {e}")
            raise

    def load_to_postgres(self, data, batch_size=1000):
        """Load data into PostgreSQL"""
        insert_query = f"""
        INSERT INTO {self.pg_table}
        (event_id, event_name, event_timestamp, user_id, session_id,
         event_data, user_properties, device_info)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
            event_name = EXCLUDED.event_name,
            event_timestamp = EXCLUDED.event_timestamp,
            user_id = EXCLUDED.user_id,
            session_id = EXCLUDED.session_id,
            event_data = EXCLUDED.event_data,
            user_properties = EXCLUDED.user_properties,
            device_info = EXCLUDED.device_info
        """

        try:
            logger.info("Loading data to PostgreSQL...")

            with self.pg_conn.cursor() as cursor:
                batch = []
                row_count = 0

                for row in data:
                    batch.append((
                        row.event_id,
                        row.event_name,
                        row.event_timestamp,
                        row.user_id,
                        row.session_id,
                        row.event_data,
                        row.user_properties,
                        row.device_info
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
        description='Extract application events from BigQuery',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract last 7 days to CSV
  python extract_bq.py --days 7 --output events.csv

  # Extract specific date range
  python extract_bq.py --from 2024-01-01 --to 2024-01-31 --output jan_events.csv

  # Extract specific events only
  python extract_bq.py --from 2024-01-01 --to 2024-01-31 --events purchase add_to_cart --output purchases.csv

  # Load directly to PostgreSQL
  python extract_bq.py --from 2024-01-01 --to 2024-01-31 --events login --postgres

  # Extract with custom filter
  python extract_bq.py --from 2024-01-01 --to 2024-01-31 --filter "platform = 'iOS'" --output ios_events.csv
        """
    )

    # Date range options
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument('--from', dest='date_from', help='Start date (YYYY-MM-DD)')
    date_group.add_argument('--days', type=int, help='Extract last N days')

    parser.add_argument('--to', dest='date_to', help='End date (YYYY-MM-DD, default: today)')

    # Event filtering
    parser.add_argument('--events', nargs='+', help='Specific event names to extract (space-separated)')
    parser.add_argument('--filter', help='Additional SQL WHERE conditions')

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
        data, query = extractor.extract_data(
            date_from=date_from,
            date_to=date_to,
            events=args.events,
            additional_filters=args.filter
        )

        # Export or load data
        if args.postgres:
            extractor.connect_postgres()
            rows_processed = extractor.load_to_postgres(data, args.batch_size)
        else:
            rows_processed = extractor.export_to_csv(data, args.output)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"\nExtraction completed successfully!")
        logger.info(f"Date range: {date_from} to {date_to}")
        logger.info(f"Total rows: {rows_processed}")
        logger.info(f"Duration: {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)
    finally:
        extractor.close_connections()


if __name__ == "__main__":
    main()