# Seattle Crime Dashboard

A interactive dashboard for visualizing Seattle crime data using Dash and Databricks.

## Features

- 📅 Date picker for selecting specific days
- 📊 KPI cards showing total, violent, property, and other crimes
- 🥧 Pie chart showing crime type distribution
- 📈 Bar chart showing crimes by time of day
- 🗺️ Interactive map with polygon filtering
- 🎯 Filter by precinct, sector, hour, and crime category
- 📋 Detailed data table

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
DATABRICKS_WAREHOUSE_ID=your_warehouse_id
DATABRICKS_CONFIG_PROFILE=data511
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
```

### 3. Configure Databricks Authentication

Add your Databricks profile to `~/.databrickscfg`:

```ini
[data511]
host  = https://your-workspace.cloud.databricks.com
token = dapi...your_token
```

## Running the App

### Local Mode (CSV)

The app automatically detects if `crime_sample.csv` exists and uses it for testing:

```bash
python app.py
```

This will run with the sample data (100 records) without needing Databricks connection.

### Production Mode (Databricks)

To use Databricks data, simply rename or remove `crime_sample.csv`:

```bash
mv crime_sample.csv crime_sample.csv.bak
python app.py
```

The app will connect to your Databricks SQL warehouse and query the `crimes` table.

## Data Schema

The app expects a table with the following columns:

- `date` - Date of the crime (DATE)
- `time` - Time of the crime (TIME or STRING)
- `hour` - Hour of the day (0-23) (INT)
- `offense` - Type of crime (STRING)
- `location` - Location description (STRING)
- `area` - Area name (STRING)
- `precinct` - Police precinct (STRING)
- `sector` - Police sector (STRING)
- `latitude` - Latitude coordinate (DOUBLE)
- `longitude` - Longitude coordinate (DOUBLE)

## Usage

1. Open your browser to `http://localhost:8050`
2. Select a date from the date picker
3. Use filters to narrow down results:
   - Hour slider: Filter by specific hour
   - Precinct/Sector dropdowns: Filter by location
   - Crime type checkboxes: Filter by category
4. Draw a polygon on the map to filter crimes within that area
5. View detailed statistics in the KPI cards and charts
6. Scroll down to see the detailed data table

## Team

**WatchDawg**: Doyoung Jung, Wonjoon Hwang, Aneesh Singh, DH Lee, Jungmoon Ha, Jonathan Langley Grothe, Derek Tropf





