# BigQuery to PostgreSQL ETL

ETL script to export application events from Google BigQuery to a local PostgreSQL database.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Update the `.env` file with your actual credentials:

**BigQuery Configuration:**
- `BQ_PROJECT_ID`: Your Google Cloud Project ID
- `BQ_DATASET`: BigQuery dataset name (e.g., analytics_123456789)
- `BQ_TABLE`: BigQuery table name (e.g., events_* for all event tables)
- `BQ_CREDENTIALS_PATH`: Path to your GCP service account JSON key file

**PostgreSQL Configuration:**
- `PG_HOST`: PostgreSQL host (default: localhost)
- `PG_PORT`: PostgreSQL port (default: 5432)
- `PG_DATABASE`: PostgreSQL database name
- `PG_USER`: PostgreSQL username
- `PG_PASSWORD`: PostgreSQL password
- `PG_TABLE`: Target table name (default: application_events)

**ETL Configuration:**
- `BATCH_SIZE`: Number of rows to insert per batch (default: 1000)

### 3. BigQuery Service Account

Create a service account in GCP with BigQuery permissions:
1. Go to GCP Console > IAM & Admin > Service Accounts
2. Create a new service account
3. Grant it "BigQuery Data Viewer" and "BigQuery Job User" roles
4. Create and download a JSON key
5. Update `BQ_CREDENTIALS_PATH` in .env with the path to this file

### 4. PostgreSQL Setup

Ensure PostgreSQL is running locally:

```bash
# Check if PostgreSQL is running
psql -U postgres -c "SELECT version();"

# Create a database if needed
createdb your_postgres_database
```

## Usage

### Option 1: Flexible Extraction Script (extract_bq.py)

Extract data with custom date ranges and event filters:

**Extract last 7 days to CSV:**
```bash
python extract_bq.py --days 7 --output events.csv
```

**Extract specific date range:**
```bash
python extract_bq.py --from 2024-01-01 --to 2024-01-31 --output jan_events.csv
```

**Extract specific events only:**
```bash
python extract_bq.py --from 2024-01-01 --to 2024-01-31 --events purchase add_to_cart checkout --output purchases.csv
```

**Load directly to PostgreSQL:**
```bash
python extract_bq.py --from 2024-01-01 --to 2024-01-31 --postgres
```

**Extract with custom filters:**
```bash
python extract_bq.py --from 2024-01-01 --to 2024-01-31 --filter "platform = 'iOS'" --output ios_events.csv
```

**Filter specific events and load to PostgreSQL:**
```bash
python extract_bq.py --from 2024-01-01 --to 2024-01-31 --events login signup --postgres
```

**Command-line options:**
- `--from DATE` or `--days N`: Specify date range (required)
- `--to DATE`: End date (default: today)
- `--events EVENT1 EVENT2 ...`: Filter specific event names
- `--filter "SQL_CONDITION"`: Additional SQL WHERE conditions
- `--output FILE` or `-o FILE`: Export to CSV file
- `--postgres`: Load directly to PostgreSQL database
- `--batch-size N`: Batch size for PostgreSQL (default: 1000)
- `--debug`: Enable debug logging

### Option 2: Full ETL Script (ETL.py)

Run the complete ETL process (default: last 30 days):

```bash
python ETL.py
```

### Database Schema

The script automatically creates the following table structure in PostgreSQL:

```sql
CREATE TABLE application_events (
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
```

Indexes are created on:
- event_timestamp
- user_id
- event_name

### Customization

You can modify the BigQuery query in the `extract_from_bigquery()` method to:
- Change the date range (default: last 30 days)
- Filter specific events
- Add custom transformations

Example:

```python
custom_query = """
SELECT
    event_id,
    event_name,
    event_timestamp,
    user_id
FROM `your-project.analytics_123456789.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20240101' AND '20240131'
    AND event_name = 'purchase'
"""

etl = BigQueryToPostgresETL()
etl.run(custom_query)
```

## Features

**extract_bq.py (Flexible Extraction):**
- Command-line interface for easy use
- Custom date range selection (from-to dates or last N days)
- Event name filtering (extract specific events)
- Custom SQL WHERE conditions
- Export to CSV or load directly to PostgreSQL
- Multiple date format support
- Debug mode for troubleshooting

**ETL.py (Full Pipeline):**
- Batch processing for efficient data loading
- Automatic table creation with indexes
- Upsert logic (INSERT ON CONFLICT UPDATE) to handle duplicates
- Comprehensive logging
- Error handling and connection management
- JSONB support for complex event data

## Troubleshooting

**Authentication Error:**
- Verify your service account JSON key is valid
- Check that the service account has proper BigQuery permissions

**Connection Error:**
- Ensure PostgreSQL is running
- Verify credentials in .env file
- Check if the database exists

**Schema Mismatch:**
- Modify the table schema in `create_postgres_table()` to match your BigQuery structure
- Update the SELECT query in `extract_from_bigquery()` accordingly
