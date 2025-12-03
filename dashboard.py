#!/usr/bin/env python3
"""
Seattle Crime Data Dashboard (Dash)
- Reads from DuckDB database (crime.duckdb)
- Provides: date picker, KPI cards, offense distribution pie chart,
            time-of-day bar chart, map, and details table.

Run:
  pip install dash dash-bootstrap-components plotly pandas duckdb
  python dashboard.py
Then open http://127.0.0.1:8050

Make sure crime.duckdb exists (run ingeest_duckdb.py first)
"""

import os
import requests
from datetime import datetime

import pandas as pd
import plotly.express as px
import dash
from dash import Dash, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table
import duckdb
import dash_leaflet as dl

DB = "crime.duckdb"
DB_URL = os.environ.get("DB_URL")


def get_google_drive_download_url(url: str) -> str:
    """Convert Google Drive sharing URL to direct download URL."""
    # Handle various Google Drive URL formats
    if "drive.google.com" in url:
        # Extract file ID from different URL formats
        if "/file/d/" in url:
            # Format: https://drive.google.com/file/d/{FILE_ID}/view...
            file_id = url.split("/file/d/")[1].split("/")[0]
        elif "id=" in url:
            # Format: https://drive.google.com/uc?id={FILE_ID}...
            file_id = url.split("id=")[1].split("&")[0]
        else:
            return url  # Return as-is if format is unknown
        return f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    return url


def download_from_google_drive(url: str, destination: str):
    """Download a file from Google Drive, handling large file confirmation."""
    session = requests.Session()
    
    response = session.get(url, stream=True)
    response.raise_for_status()
    
    # Check if we got a confirmation page (for large files)
    # Google Drive returns HTML with a confirmation token for large files
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type:
        # Try to find confirmation token in cookies
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                # Add confirmation token to URL
                url = f"{url}&confirm={value}"
                response = session.get(url, stream=True)
                response.raise_for_status()
                break
    
    # Write the file
    with open(destination, "wb") as f:
        for chunk in response.iter_content(1024 * 1024):  # 1MB chunks
            if chunk:
                f.write(chunk)


def ensure_db():
    """Download DuckDB file from DB_URL if it doesn't exist locally."""
    if os.path.exists(DB):
        print(f"[DB] Using local DB: {DB}")
        return
    if not DB_URL:
        print("[DB] DB_URL is not set. DB file will not be downloaded.")
        return
    print(f"[DB] Downloading DB from {DB_URL} ...")
    try:
        # Convert Google Drive URL if needed
        download_url = get_google_drive_download_url(DB_URL)
        
        if "drive.google.com" in DB_URL:
            download_from_google_drive(download_url, DB)
        else:
            resp = requests.get(download_url, stream=True)
            resp.raise_for_status()
            with open(DB, "wb") as f:
                for chunk in resp.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
        print("[DB] Download complete.")
    except Exception as e:
        print(f"[DB] Failed to download DB: {e}")

def get_date_range():
    """Get min and max date from DuckDB."""
    try:
        with duckdb.connect(DB, read_only=True) as con:
            result = con.execute("SELECT MIN(date), MAX(date) FROM crimes").fetchone()
            if result and result[0] and result[1]:
                return {'min': str(result[0]), 'max': str(result[1])}
    except Exception as e:
        print(f"Error getting date range: {e}")
    return {'min': '', 'max': ''}

def get_precincts():
    """Get distinct precincts from DuckDB."""
    try:
        with duckdb.connect(DB, read_only=True) as con:
            rows = con.execute(
                "SELECT DISTINCT precinct FROM crimes WHERE precinct IS NOT NULL AND precinct <> '' ORDER BY precinct"
            ).fetchall()
            return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        print(f"Error getting precincts: {e}")
    return []

def get_sectors():
    """Get distinct sectors from DuckDB."""
    try:
        with duckdb.connect(DB, read_only=True) as con:
            rows = con.execute(
                "SELECT DISTINCT sector FROM crimes WHERE sector IS NOT NULL AND sector <> '' ORDER BY sector"
            ).fetchall()
            return [r[0] for r in rows if r and r[0]]
    except Exception as e:
        print(f"Error getting sectors: {e}")
    return []

def get_kpis(day: str):
    """Get KPI statistics for a specific day."""
    sql = """
    SELECT 
        COUNT(*) AS total,
        SUM(CASE WHEN offense LIKE '%ASSAULT%' OR offense LIKE '%ROBBERY%' OR
                     offense LIKE '%HOMICIDE%' OR offense LIKE '%RAPE%' OR
                     offense LIKE '%SEX OFFENSE%' THEN 1 ELSE 0 END) AS violent,
        SUM(CASE WHEN offense LIKE '%THEFT%' OR offense LIKE '%BURGLARY%' OR
                     offense LIKE '%CAR PROWL%' OR offense LIKE '%SHOPLIFTING%' OR
                     offense LIKE '%MOTOR VEHICLE THEFT%' OR offense LIKE '%ARSON%' OR
                     offense LIKE '%VANDALISM%' THEN 1 ELSE 0 END) AS property,
        COUNT(*) - (
            SUM(CASE WHEN offense LIKE '%ASSAULT%' OR offense LIKE '%ROBBERY%' OR
                         offense LIKE '%HOMICIDE%' OR offense LIKE '%RAPE%' OR
                         offense LIKE '%SEX OFFENSE%' THEN 1 ELSE 0 END) +
            SUM(CASE WHEN offense LIKE '%THEFT%' OR offense LIKE '%BURGLARY%' OR
                         offense LIKE '%CAR PROWL%' OR offense LIKE '%SHOPLIFTING%' OR
                         offense LIKE '%MOTOR VEHICLE THEFT%' OR offense LIKE '%ARSON%' OR
                         offense LIKE '%VANDALISM%' THEN 1 ELSE 0 END)
        ) AS other
    FROM crimes
    WHERE date = ?
    """
    with duckdb.connect(DB, read_only=True) as con:
        row = con.execute(sql, [day]).fetchone()
        if row:
            return (row[0] or 0, row[1] or 0, row[2] or 0, row[3] or 0)
        return (0, 0, 0, 0)

def get_top_offenses(day: str, topn=10) -> pd.DataFrame:
    """Get top offenses for a specific day."""
    sql = """
      SELECT offense AS type, COUNT(*) AS count
      FROM crimes
      WHERE date = ?
      GROUP BY offense
      ORDER BY count DESC
      LIMIT ?
    """
    with duckdb.connect(DB, read_only=True) as con:
        return con.execute(sql, [day, topn]).df()

def get_hour_hist(day: str) -> pd.DataFrame:
    """Get hourly histogram for a specific day."""
    sql = """
      SELECT hour AS Hour, COUNT(*) AS Count
      FROM crimes
      WHERE date = ?
      GROUP BY hour
      ORDER BY hour
    """
    with duckdb.connect(DB, read_only=True) as con:
        df = con.execute(sql, [day]).df()
        # Ensure all 24 hours are present (fill missing with 0)
        if not df.empty:
            all_hours = pd.DataFrame({'Hour': range(24)})
            df = all_hours.merge(df, on='Hour', how='left').fillna(0)
            df['Count'] = df['Count'].astype(int)
        else:
            df = pd.DataFrame({'Hour': range(24), 'Count': [0]*24})
        return df

def get_map_points(day: str) -> pd.DataFrame:
    with duckdb.connect(DB, read_only=True) as con:
        sql = """
          SELECT latitude, longitude, offense, location, area, precinct, sector, time
          FROM crimes
          WHERE date = ? AND latitude IS NOT NULL AND longitude IS NOT NULL
        """
        return con.execute(sql, [day]).df()


def get_table(day: str, limit=500) -> pd.DataFrame:
    """Get table data for a specific day."""
    sql = """
      SELECT date, time, offense, location, area, precinct, sector
      FROM crimes
      WHERE date = ?
      ORDER BY time DESC
      LIMIT ?
    """
    with duckdb.connect(DB, read_only=True) as con:
        return con.execute(sql, [day, limit]).df()

# ---------------------------- Spatial Functions ----------------------------

def _connect():
    """Open a read-only DuckDB connection and ensure spatial extension is loaded."""
    con = duckdb.connect(DB, read_only=True)
    # INSTALL is cached after first time; safe to call.
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    return con

def geojson_to_wkt_polygon(geojson: dict) -> str:
    """
    Convert a Leaflet polygon GeoJSON to WKT POLYGON string.
    Assumes coordinates in [lon, lat] order (Leaflet/GeoJSON spec).
    Ensures the ring is closed (first == last).
    """
    coords = geojson["geometry"]["coordinates"][0]
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    # WKT needs "lon lat" pairs separated by commas.
    ring = ", ".join([f"{lon} {lat}" for lon, lat in coords])
    return f"POLYGON(({ring}))"

def query_points_in_polygon(day: str, wkt_polygon: str) -> pd.DataFrame:
    """
    Return points strictly inside the given polygon for the specified date.
    No sampling - returns all matching points.
    """
    sql = """
      SELECT
        date, time, hour, offense, location, area, precinct, sector, latitude, longitude
      FROM crimes
      WHERE date = ?
        AND latitude IS NOT NULL AND longitude IS NOT NULL
        AND ST_Within(
              ST_Point(longitude, latitude),
              ST_GeomFromText(?)
            )
    """
    with _connect() as con:
        return con.execute(sql, [day, wkt_polygon]).df()

def day_overview(day: str, topn: int = 10) -> pd.DataFrame:
    """Return all points for the whole day (when no polygon drawn). No sampling."""
    sql = """
      SELECT date, time, hour, offense, location, area, precinct, sector, latitude, longitude
      FROM crimes
      WHERE date = ?
        AND latitude IS NOT NULL AND longitude IS NOT NULL
    """
    with _connect() as con:
        return con.execute(sql, [day]).df()


# ---------------------------- App ----------------------------

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server  # Expose Flask server for gunicorn (Render deployment)
app.title = "Seattle Crime Dashboard"

# Get initial date range from DB
initial_date_range = get_date_range()
precinct_choices = get_precincts()
sector_choices = get_sectors()

header = html.Div(
    [
        html.Div(
            [
                html.Img(
                    src="/assets/watchdawg_logo.png",
                    style={
                        "height": "180px",
                        "display": "block",
                        "margin": "0 auto",
                        "borderRadius": "12px",
                        "padding": "6px",
                        "backgroundColor": "white" 
                    }
                ),

                html.H1(
                    "WatchDawg Crime Analytics",
                    className="text-center",
                    style={
                        "fontWeight": "700",
                        "fontSize": "2.4rem",
                        "color": "#4B2E83",  # UW Purple
                        "marginTop": "12px"
                    }
                ),

                html.P(
                    [
                        "Real-time incident visualization & statistical insights.",
                        html.Br(),
                        html.Span("By WatchDawg: Doyoung Jung, Wonjoon Hwang, Aneesh Singh, DH Lee, Jungmoon Ha, Jonathan Langley Grothe, Derek Tropf",
                            style={"fontWeight": "600", "color": "#B7A57A"})  
                    ],
                    className="text-center",
                    style={"fontSize": "1.05rem", "color": "#444"}
                ),
            ],
            style={
                "textAlign": "center",
                "padding": "28px 20px",
                "border": "3px solid #B7A57A",  
                "borderRadius": "14px",
                "background": "rgba(255,255,255,0.92)",
                "backdropFilter": "blur(8px)",
                "boxShadow": "0 10px 28px rgba(0,0,0,0.15)"
            }
        ),
        html.Hr()
    ],
    style={"marginBottom": "24px"}
)

# Date picker row
date_row = dbc.Row(
    [
        dbc.Col(
            [
                html.Label("📅 Select Date", className="fw-bold"),
                dcc.DatePickerSingle(
                    id="date-picker",
                    display_format="YYYY-MM-DD",
                    clearable=False,
                    min_date_allowed=datetime.strptime(initial_date_range['min'], "%Y-%m-%d").date() if initial_date_range['min'] else None,
                    max_date_allowed=datetime.strptime(initial_date_range['max'], "%Y-%m-%d").date() if initial_date_range['max'] else None,
                    date=datetime.strptime(initial_date_range['max'], "%Y-%m-%d").date() if initial_date_range['max'] else None,
                ),
                html.Div(
                    id="date-range-label",
                    children=f"Available: {initial_date_range['min']} → {initial_date_range['max']}" if initial_date_range['min'] and initial_date_range['max'] else "No data available",
                    className="text-muted mt-1"
                ),
            ],
            md=4
        ),
        dbc.Col(html.Div(), md=8),
    ],
    className="my-3"
)

# KPI cards
kpi_cards = dbc.Row(
    [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Total Crimes", className="text-muted"),
                    html.H2(id="kpi-total", className="mb-0 text-primary")
                ]),
                className="text-center"
            ),
            md=3
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Violent", className="text-muted"),
                    html.H2(id="kpi-violent", className="mb-0 text-danger")
                ]),
                className="text-center"
            ),
            md=3
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Property", className="text-muted"),
                    html.H2(id="kpi-property", className="mb-0 text-warning")
                ]),
                className="text-center"
            ),
            md=3
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Other", className="text-muted"),
                    html.H2(id="kpi-other", className="mb-0 text-info")
                ]),
                className="text-center"
            ),
            md=3
        ),
    ],
    className="g-3 mb-3"
)

# Charts row
charts_row = dbc.Row(
    [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H5("Crime Type Distribution", className="card-title"),
                    dcc.Graph(id="pie-offense")
                ])
            ),
            md=6
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H5("Crimes by Time of Day", className="card-title"),
                    dcc.Graph(id="bar-hourly")
                ])
            ),
            md=6
        ),
    ],
    className="g-3 mb-3"
)

# Map and table
map_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("🗺️ Crime Map", className="card-title"),
            html.P(
                "Draw a polygon on the map to filter crimes by area. Use the hour slider and crime type filter to refine results.",
                className="text-muted mb-2"
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.Label("⏰ Hour filter", className="fw-semibold"),
                                    dcc.Slider(
                                        id="hour-slider",
                                        min=0,
                                        max=24,
                                        step=1,
                                        value=24,
                                        marks={
                                            0: "0",
                                            6: "6",
                                            12: "12",
                                            18: "18",
                                            24: "All"
                                        },
                                        tooltip={"always_visible": False, "placement": "bottom"}
                                    ),
                                    html.Div(
                                        id="hour-label",
                                        className="text-muted small text-center mt-2",
                                    ),
                                ],
                                className="mb-3"
                            ),
                            dl.Map(
                                [
                                    dl.TileLayer(),
                                    dl.FeatureGroup([
                                        dl.EditControl(
                                            id="edit-control",
                                            position="topleft",
                                            draw=dict(
                                                polygon=True,
                                                polyline=False,
                                                rectangle=False,
                                                circle=False,
                                                marker=False,
                                                circlemarker=False
                                            ),
                                            edit=dict(remove=True)
                                        ),
                                        html.Div(id="points-layer")
                                    ])
                                ],
                                center=[47.6062, -122.3321],  # Seattle center
                                zoom=11,
                                style={"height": "650px", "width": "100%"},
                                id="map"
                            ),
                        ],
                        md=9
                    ),
                    dbc.Col(
                        [
                            html.Label("🚓 Precinct filter", className="fw-semibold"),
                            dcc.Dropdown(
                                id="precinct-filter",
                                options=[{"label": p, "value": p} for p in precinct_choices],
                                value=[],
                                placeholder="Select precinct(s)",
                                multi=True,
                                clearable=True,
                                className="mb-3"
                            ),
                            html.Label("🧭 Sector filter", className="fw-semibold"),
                            dcc.Dropdown(
                                id="sector-filter",
                                options=[{"label": s, "value": s} for s in sector_choices],
                                value=[],
                                placeholder="Select sector(s)",
                                multi=True,
                                clearable=True,
                                className="mb-3"
                            ),
                            html.Label("🎯 Crime type filter", className="fw-semibold"),
                            dcc.Checklist(
                                id="category-filter",
                                options=[
                                    {"label": "Violent", "value": "Violent"},
                                    {"label": "Property", "value": "Property"},
                                    {"label": "Other", "value": "Other"},
                                ],
                                value=["Violent", "Property", "Other"],
                                labelStyle={"display": "block", "marginBottom": "6px"},
                                inputStyle={"marginRight": "8px"}
                            ),
                            html.Div(
                                [
                                    html.Small("Colored markers correspond to the selected crime categories."),
                                    html.Ul(
                                        [
                                            html.Li(
                                                [
                                                    html.Span(
                                                        style={
                                                            "display": "inline-block",
                                                            "width": "12px",
                                                            "height": "12px",
                                                            "backgroundColor": "#d9534f",
                                                            "marginRight": "6px",
                                                            "borderRadius": "50%"
                                                        }
                                                    ),
                                                    "Violent"
                                                ],
                                                className="text-danger"
                                            ),
                                            html.Li(
                                                [
                                                    html.Span(
                                                        style={
                                                            "display": "inline-block",
                                                            "width": "12px",
                                                            "height": "12px",
                                                            "backgroundColor": "#f0ad4e",
                                                            "marginRight": "6px",
                                                            "borderRadius": "50%"
                                                        }
                                                    ),
                                                    "Property"
                                                ],
                                                className="text-warning"
                                            ),
                                            html.Li(
                                                [
                                                    html.Span(
                                                        style={
                                                            "display": "inline-block",
                                                            "width": "12px",
                                                            "height": "12px",
                                                            "backgroundColor": "#5bc0de",
                                                            "marginRight": "6px",
                                                            "borderRadius": "50%"
                                                        }
                                                    ),
                                                    "Other"
                                                ],
                                                className="text-info"
                                            ),
                                        ],
                                        className="list-unstyled mt-3"
                                    ),
                                ],
                                className="mt-3"
                            ),
                        ],
                        md=3
                    ),
                ],
                className="g-3 align-items-start flex-column flex-md-row"
            ),
            dcc.Store(id="poly-store", data=None),  # Store polygon GeoJSON
            html.Hr(),
            html.H6("📋 Details (top 500 of the selected area/day)"),
            dash_table.DataTable(
                id="details-table",
                columns=[
                    {"name": "Date", "id": "date"},
                    {"name": "Time", "id": "time"},
                    {"name": "Offense", "id": "offense"},
                    {"name": "Location", "id": "location"},
                    {"name": "Area", "id": "area"},
                    {"name": "Precinct", "id": "precinct"},
                    {"name": "Sector", "id": "sector"},
                ],
                data=[],
                page_size=10,
                style_table={"overflowX": "auto"},
                style_cell={"whiteSpace": "normal", "height": "auto", "textAlign": "left"},
                style_header={"backgroundColor": "#f8f9fa", "fontWeight": "bold"},
            )
        ]
    ),
    className="mb-4"
)

app.layout = dbc.Container(
    [
        header,
        date_row,
        kpi_cards,
        charts_row,
        map_card,
    ],
    fluid=True,
)


# ---------------------------- Callbacks ----------------------------

# Store polygon when drawn/edited
@app.callback(
    Output("poly-store", "data"),
    Input("edit-control", "geojson"),
    prevent_initial_call=True
)
def store_polygon(geojson):
    """Store the polygon GeoJSON when drawn or edited on the map."""
    if geojson and geojson.get("type") == "FeatureCollection":
        # Get the first polygon feature
        features = geojson.get("features", [])
        if features:
            for feature in features:
                if feature.get("geometry", {}).get("type") == "Polygon":
                    return feature
    return None

@app.callback(
    Output("kpi-total", "children"),
    Output("kpi-violent", "children"),
    Output("kpi-property", "children"),
    Output("kpi-other", "children"),
    Output("pie-offense", "figure"),
    Output("bar-hourly", "figure"),
    Output("points-layer", "children"),   # leaflet markers
    Output("details-table", "data"),
    Output("hour-label", "children"),
    Input("date-picker", "date"),
    Input("poly-store", "data"),
    Input("hour-slider", "value"),
    Input("category-filter", "value"),
    Input("precinct-filter", "value"),
    Input("sector-filter", "value"),
    prevent_initial_call=False
)
def update_by_polygon(selected_date, poly_geojson, hour_value, category_value, precinct_value, sector_value):
    # Normalize hour selection (24 == show all)
    selected_hour = hour_value if hour_value is not None else 24
    if selected_hour == 24:
        hour_label = "Showing all hours (00:00–23:59)"
    else:
        hour_label = f"Showing hour {selected_hour:02d}:00 – {selected_hour:02d}:59"

    selected_categories = category_value if category_value is not None else ["Violent", "Property", "Other"]

    # 1) Load data: inside polygon if drawn, otherwise the whole day
    if not selected_date:
        empty_pie = px.pie(values=[1], names=["NO DATA"], hole=0.4)
        empty_pie.update_layout(height=420, legend_title_text="Offense", showlegend=True)
        empty_bar = px.bar()
        empty_bar.update_layout(height=420, xaxis_title="Hour (0–23)", yaxis_title="Count")
        return "0","0","0","0", empty_pie, empty_bar, [], [], hour_label

    day = pd.to_datetime(selected_date).strftime("%Y-%m-%d")

    try:
        if poly_geojson:
            wkt = geojson_to_wkt_polygon(poly_geojson)
            df = query_points_in_polygon(day, wkt)
        else:
            df = day_overview(day)
    except Exception as e:
        print(f"Error querying data: {e}")
        # Safe fallback - return empty dataframe with required columns
        df = pd.DataFrame(columns=["date", "time", "hour", "offense", "location", "area", "precinct", "sector", "latitude", "longitude"])

    # Apply hour filter if a specific hour is selected
    if selected_hour != 24:
        df = df[df["hour"] == selected_hour]

    if precinct_value:
        if isinstance(precinct_value, str):
            precincts = [precinct_value]
        else:
            precincts = precinct_value
        if "precinct" in df.columns and precincts:
            df = df[df["precinct"].isin(precincts)]

    if sector_value:
        if isinstance(sector_value, str):
            sectors = [sector_value]
        else:
            sectors = sector_value
        if "sector" in df.columns and sectors:
            df = df[df["sector"].isin(sectors)]

    def is_violent(s: str) -> bool:
        s = str(s).upper()
        return any(k in s for k in ["ASSAULT","ROBBERY","HOMICIDE","RAPE","SEX OFFENSE"])
    def is_property(s: str) -> bool:
        s = str(s).upper()
        return any(k in s for k in ["THEFT","BURGLARY","CAR PROWL","SHOPLIFTING","MOTOR VEHICLE THEFT","ARSON","VANDALISM"])

    def categorize_offense(s: str) -> str:
        if is_violent(s):
            return "Violent"
        if is_property(s):
            return "Property"
        return "Other"

    df["category"] = df["offense"].apply(categorize_offense)
    df_filtered = df[df["category"].isin(selected_categories)].copy()

    # Separate copy for map markers where lat/lon is required
    df_map = df_filtered.dropna(subset=['latitude', 'longitude']).copy()

    total = len(df_filtered)
    violent = int((df_filtered["category"] == "Violent").sum()) if total else 0
    property_ = int((df_filtered["category"] == "Property").sum()) if total else 0
    other = int((df_filtered["category"] == "Other").sum()) if total else 0

    # 3) Charts - use df_filtered (include records even without coordinates)
    if total:
        df_types = df_filtered["offense"].str.upper().value_counts().head(10).reset_index()
        df_types.columns = ["type", "count"]
        fig_pie = px.pie(df_types, values="count", names="type", hole=0.4, 
                        color_discrete_sequence=px.colors.qualitative.Set3)
        fig_pie.update_layout(height=420, legend_title_text="Offense", showlegend=True)
    else:
        fig_pie = px.pie(values=[1], names=["NO DATA"], hole=0.4)
        fig_pie.update_layout(height=420, legend_title_text="Offense", showlegend=True)

    # Hourly bar
    if total:
        hh = pd.to_datetime(df_filtered["time"], errors="coerce").dt.hour.fillna(-1).astype(int)
        df_hour = pd.DataFrame({"Hour": list(range(24))})
        counts = hh[hh.between(0,23)].value_counts().reindex(range(24), fill_value=0).values
        df_hour["Count"] = counts
        fig_bar = px.bar(df_hour, x=df_hour["Hour"].astype(str), y="Count", 
                        color="Count", color_continuous_scale="Blues",
                        labels={"x": "Hour (0-23)", "Count": "Number of Crimes"})
        fig_bar.update_layout(height=420, xaxis_title="Hour (0–23)", yaxis_title="Count", 
                             xaxis_tickangle=-45)
    else:
        fig_bar = px.bar()
        fig_bar.update_layout(height=420, xaxis_title="Hour (0–23)", yaxis_title="Count")

    # 4) Leaflet markers (limit for performance) - use df_map (records with coordinates)
    markers = []
    category_colors = {
        "Violent": "#d9534f",
        "Property": "#f0ad4e",
        "Other": "#5bc0de",
    }
    if not df_map.empty:
        # Show up to 3000 markers for speed
        show = df_map.head(3000)
        for _, r in show.iterrows():
            try:
                lat = float(r["latitude"])
                lon = float(r["longitude"])
                category = r.get("category", "Other")
                color = category_colors.get(category, "#636efa")
                # Validate coordinates are within reasonable range
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    tooltip_parts = [
                        category,
                        r.get('time', ''),
                        r.get('offense', '')
                    ]
                    if r.get('precinct'):
                        tooltip_parts.append(f"Precinct: {r.get('precinct', '')}")
                    if r.get('sector'):
                        tooltip_parts.append(f"Sector: {r.get('sector', '')}")
                    if r.get('area'):
                        tooltip_parts.append(r.get('area', ''))
                    markers.append(
                        dl.CircleMarker(
                            center=(lat, lon),
                            radius=6,
                            color=color,
                            fill=True,
                            fillColor=color,
                            fillOpacity=0.85,
                            weight=1,
                            children=dl.Tooltip(
                                " | ".join([p for p in tooltip_parts if p])
                            )
                        )
                    )
            except (ValueError, TypeError):
                # Skip rows with invalid coordinates
                continue

    # 5) Table (top 500) - use df_valid to match what's shown
    table_columns = [col for col in ["date", "time", "offense", "location", "area", "precinct", "sector"] if col in df_filtered.columns]
    table = df_filtered[table_columns].head(500).to_dict("records")

    return (
        f"{total:,}", f"{violent:,}", f"{property_:,}", f"{other:,}",
        fig_pie, fig_bar, markers, table, hour_label
    )


ensure_db()

if __name__ == "__main__":
    # Check if DB exists
    if not os.path.exists(DB):
        print(f"⚠️  Warning: Database file '{DB}' not found!")
        print(f"   Please run 'ingeest_duckdb.py' first to create the database.")
        print()
    
    port = int(os.environ.get('PORT', 8050))
    print(f"Server running at http://localhost:{port}")
    print("Browser will open automatically. Press Ctrl+C to stop the server.")
    
    # Prevent Dash from automatically opening browser with wrong URL
    os.environ['DASH_OPEN_BROWSER'] = '0'
    
    # Open browser manually with correct URL
    import webbrowser
    import threading
    def open_browser():
        import time
        time.sleep(1.5)  # Wait for server to start
        webbrowser.open(f'http://localhost:{port}')
    debug_mode = os.environ.get("DASH_DEBUG", "1") == "1"
    should_open_browser = True
    if debug_mode and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        should_open_browser = False

    if should_open_browser:
        threading.Thread(target=open_browser, daemon=True).start()
    
    app.run(host='0.0.0.0', port=port, debug=debug_mode)

