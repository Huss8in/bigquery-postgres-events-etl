# BigQuery to PostgreSQL ETL

Two simple scripts for moving data from BigQuery to PostgreSQL.

## Scripts

### 1. flask_server.py - Automated Incremental ETL
Flask server that runs timestamp-based incremental ETL automatically at scheduled intervals.

**Features:**
- **Incremental processing:** Uses event timestamps to track progress and resume from last processed point
- **Resilient to failures:** Stores last processed timestamp in `last_timestamp.txt` - if ETL fails, next run continues from same point
- **Configurable lookback window:** On first run, looks back N hours (default: 24) from current time
- Runs daily at 2:00 AM (configurable via .env)
- Keeps all old data (append-only)
- Provides API endpoints for monitoring and manual triggers

**Usage:**
```bash
python flask_server.py
```

**API Endpoints:**
- `GET /` - Service info
- `GET /health` - Health check
- `GET /status` - ETL status and last run info
- `POST /trigger` - Manually trigger ETL run

### 2. extract_bq.py - Manual BigQuery Extraction Tool
Extract data from BigQuery with custom date ranges and event filters. Can export to CSV or load directly to PostgreSQL.

**Usage:**
```bash
# Extract last 7 days to CSV (default events)
python extract_bq.py --days 7 --output events.csv

# Extract specific date range with default events
python extract_bq.py --from 2026-01-01 --to 2026-01-10 --output jan_events.csv

# Extract with specific events
python extract_bq.py --from 2026-01-01 --to 2026-01-10 --events view_item add_to_cart --output events.csv

# Load directly to PostgreSQL
python extract_bq.py --from 2026-01-01 --to 2026-01-10 --postgres

# Extract yesterday's data
python extract_bq.py --days 1 --output yesterday.csv

# Show help
python extract_bq.py --help
```

## Configuration

Create a `.env` file with:

```env
# BigQuery
BQ_PROJECT_ID=your-project-id
BQ_DATASET=your-dataset
BQ_TABLE_PREFIX=events_
BQ_CREDENTIALS_PATH=your-credentials.json
BQ_LOCATION=europe-west1

# PostgreSQL
PG_HOST=localhost
PG_PORT=5433
PG_DATABASE=postgres
PG_USER=postgres
PG_PASSWORD=your-password
PG_TABLE=application_events

# Flask Server
FLASK_PORT=5000
ETL_SCHEDULE_HOUR=2
ETL_SCHEDULE_MINUTE=0
ETL_LOOKBACK_HOURS=24
```

## Installation

### Standard Installation
```bash
pip install -r requirements.txt
```

### Docker Installation

**Prerequisites:**
- Docker and Docker Compose installed
- BigQuery service account credentials JSON file
- PostgreSQL database accessible from Docker container

**Quick Start:**

1. **Configure environment variables** - Update `.env` file with your settings:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Create data directory for state persistence:**
   ```bash
   mkdir -p data
   ```

3. **Build and run the container:**
   ```bash
   docker-compose up -d
   ```

4. **View logs:**
   ```bash
   docker-compose logs -f flask-etl
   ```

5. **Check status:**
   ```bash
   curl http://localhost:5000/status
   ```

6. **Manually trigger ETL:**
   ```bash
   curl -X POST http://localhost:5000/trigger
   ```

7. **Stop the container:**
   ```bash
   docker-compose down
   ```

**Docker Build Only:**
```bash
docker build -t bigquery-postgres-etl .
docker run -d \
  --name etl-server \
  -p 5000:5000 \
  -v $(pwd)/data:/app/data \
  -v /path/to/credentials.json:/app/credentials.json:ro \
  --env-file .env \
  bigquery-postgres-etl
```

## Notes

### General
- Both scripts append data (don't delete old records)
- Duplicate records are skipped automatically (based on user_id, event_timestamp, event_name)
- **flask_server.py** uses timestamp-based incremental processing:
  - Stores progress in `last_timestamp.txt` (or `/app/data/last_timestamp.txt` in Docker)
  - On failure, retries from last successful timestamp
  - On first run, looks back N hours (configured by `ETL_LOOKBACK_HOURS`)

### Docker-Specific
- State file (`last_timestamp.txt`) is persisted in mounted `./data` directory
- BigQuery credentials file is mounted as read-only volume
- Container includes health checks for monitoring
- Auto-restarts on failure with `restart: unless-stopped` policy
- All configuration via `.env` file - no code changes needed
- Logs accessible via `docker-compose logs -f flask-etl`

### Troubleshooting
- **PostgreSQL connection issues in Docker:** Ensure `PG_HOST` is accessible from container (use host IP or Docker network)
- **Credentials not found:** Check that `BQ_CREDENTIALS_PATH` in `.env` points to correct file location
- **Permission errors on data directory:** Ensure `./data` directory has proper write permissions
