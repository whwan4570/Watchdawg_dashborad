# Seattle Crime Data Dashboard

Interactive web dashboard for viewing Seattle crime data using DuckDB.

## Files

### `dashboard.py`

Dash-based web application that displays:

- KPI cards (Total, Violent, Property, Other crimes)
- Crime type distribution pie chart
- Hourly crime bar chart
- Interactive Leaflet map with polygon filtering
- Crime details table

**Run:**

```bash
python dashboard.py
```

Opens at `http://localhost:8050`

### `ingeest_duckdb.py`

Ingests Seattle crime CSV data into DuckDB database:

- Parses CSV and normalizes columns
- Extracts date, time, offense, location, area, coordinates
- Filters to 2008+ and Seattle bounding box
- Creates `crime.duckdb` database

**Run:**

```bash
python ingeest_duckdb.py
```

### `data_processor.py`

CSV parsing and data processing utilities:

- Parses multiple datetime formats
- Normalizes offense, location, area fields
- Validates and filters coordinates
- Calculates statistics and categorizes crimes

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Ingest data into DuckDB:

   ```bash
   python ingeest_duckdb.py
   ```
2. Run the dashboard:

   ```bash
   python dashboard.py
   ```
3. Draw polygons on the map to filter crimes by area
