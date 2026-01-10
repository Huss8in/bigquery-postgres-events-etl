import os
import logging
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class BigQueryToPostgresETL:
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

        # ETL Configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))

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

    def create_postgres_table(self):
        """Create the target table in PostgreSQL if it doesn't exist"""
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.pg_table} (
            event_id VARCHAR(255) PRIMARY KEY,
            event_name VARCHAR(255),
            event_timestamp TIMESTAMP,
            user_id VARCHAR(255),
            session_id VARCHAR(255),
            event_data JSONB,
            user_properties JSONB,
            device_info JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_{self.pg_table}_timestamp
        ON {self.pg_table}(event_timestamp);

        CREATE INDEX IF NOT EXISTS idx_{self.pg_table}_user_id
        ON {self.pg_table}(user_id);

        CREATE INDEX IF NOT EXISTS idx_{self.pg_table}_event_name
        ON {self.pg_table}(event_name);
        """

        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.pg_conn.commit()
            logger.info(f"Table {self.pg_table} created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            self.pg_conn.rollback()
            raise

    def extract_from_bigquery(self, query=None):
        """Extract data from BigQuery"""
        if query is None:
            query = f"""
            SELECT
                event_id,
                event_name,
                event_timestamp,
                user_id,
                session_id,
                TO_JSON_STRING(event_params) as event_data,
                TO_JSON_STRING(user_properties) as user_properties,
                TO_JSON_STRING(device) as device_info
            FROM `{self.bq_project_id}.{self.bq_dataset}.{self.bq_table}`
            WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ORDER BY event_timestamp DESC
            """

        try:
            logger.info("Extracting data from BigQuery...")
            query_job = self.bq_client.query(query)
            results = query_job.result()

            total_rows = results.total_rows
            logger.info(f"Extracted {total_rows} rows from BigQuery")

            return results
        except Exception as e:
            logger.error(f"Failed to extract data from BigQuery: {e}")
            raise

    def load_to_postgres(self, data):
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

                    if len(batch) >= self.batch_size:
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

    def run(self, custom_query=None):
        """Execute the complete ETL process"""
        start_time = datetime.now()
        logger.info("Starting ETL process...")

        try:
            # Connect to databases
            self.connect_bigquery()
            self.connect_postgres()

            # Create table if not exists
            self.create_postgres_table()

            # Extract data from BigQuery
            data = self.extract_from_bigquery(custom_query)

            # Load data to PostgreSQL
            rows_loaded = self.load_to_postgres(data)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"ETL process completed successfully!")
            logger.info(f"Total rows processed: {rows_loaded}")
            logger.info(f"Duration: {duration:.2f} seconds")

        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        finally:
            # Close connections
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("PostgreSQL connection closed")
            if self.bq_client:
                logger.info("BigQuery client closed")


def main():
    """Main execution function"""
    etl = BigQueryToPostgresETL()

    # You can optionally provide a custom query
    # custom_query = "SELECT * FROM `project.dataset.table` WHERE ..."

    etl.run()


if __name__ == "__main__":
    main()
