#!/usr/bin/env python3

"""
Seattle Crime Data Dashboard (Dash)
- Reads from local CSV file (crime_data_gold.csv)
- Provides: date picker, KPI cards, offense distribution pie chart,
            time-of-day bar chart, map, and details table.

Run:
  pip install -r requirements.txt
  python app.py
Then open http://127.0.0.1:8050
"""

import os
import time

import pandas as pd
import numpy as np
import plotly.express as px
import dash
from dash import Dash, html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc
from dash import dash_table
import dash_mantine_components as dmc

# Data file paths (relative to app.py)
PARQUET_FILE_PATH = os.path.join(os.path.dirname(__file__), 'crime_data_gold.parquet')
CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), 'crime_data_gold.csv')

# Data cache - stores all data in memory for fast access
_data_cache = None
_cache_timestamp = None

def load_from_parquet():
    """Load data from Parquet file."""
    try:
        if not os.path.exists(PARQUET_FILE_PATH):
            print(f"❌ Parquet file not found: {PARQUET_FILE_PATH}")
            return None
        
        print(f"🔄 Loading data from Parquet file: {PARQUET_FILE_PATH}")
        df = pd.read_parquet(PARQUET_FILE_PATH)
        print(f"📊 Read {len(df)} rows from Parquet")
        print(f"📋 Columns in Parquet file: {list(df.columns)}")
        
        # Check if Parquet has already processed columns (lowercase with underscores)
        # or original CSV columns (with spaces and capitals)
        if 'latitude' in df.columns and 'longitude' in df.columns:
            # Already processed - just filter
            df = df[df['latitude'].notna() & df['longitude'].notna()].copy()
            print(f"📍 After filtering for valid coordinates: {len(df)} records")
            
            # Ensure datetime column exists
            if 'datetime' not in df.columns and 'date' in df.columns and 'time' in df.columns:
                df['datetime'] = df['date'].astype(str) + ' ' + df['time'].astype(str)
        elif 'Latitude' in df.columns and 'Longitude' in df.columns:
            # Original CSV format - need to process like CSV
            df = df[df['Latitude'].notna() & df['Longitude'].notna()].copy()
            print(f"📍 After filtering for valid coordinates: {len(df)} records")
            
            # Create derived columns to match the expected schema
            df['date'] = pd.to_datetime(
                df['Offense Year'].astype(int).astype(str) + '-' + 
                df['Offense Month'].astype(int).astype(str).str.zfill(2) + '-' + 
                df['Offense Day'].astype(int).astype(str).str.zfill(2),
                format='%Y-%m-%d',
                errors='coerce'
            )
            
            df['time'] = df['Offense Time'].astype(str)
            df['hour'] = pd.to_datetime(df['Offense Time'], format='%H:%M:%S', errors='coerce').dt.hour
            df['hour'] = df['hour'].fillna(0).astype(int)
            df['datetime'] = df['date'].astype(str) + ' ' + df['Offense Time'].astype(str)
            
            # Rename columns
            df = df.rename(columns={
                'Offense Category': 'offense',
                'Offense Sub Category': 'offense_sub_category',
                'NIBRS Crime Against Category': 'crime_against_category',
                'Block Address': 'location',
                'Neighborhood': 'area',
                'Precinct': 'precinct',
                'Sector': 'sector',
                'Hazardness': 'hazardness',
                'Latitude': 'latitude',
                'Longitude': 'longitude'
            })
            
            # Select only the columns we need
            df = df[['date', 'time', 'hour', 'datetime', 'offense', 'offense_sub_category',
                     'crime_against_category', 'location', 'area', 'precinct', 'sector',
                     'hazardness', 'latitude', 'longitude']].copy()
        else:
            print("⚠️  Unexpected column structure in Parquet file")
            print(f"   Available columns: {list(df.columns)}")
            print("   Expected either 'latitude'/'longitude' or 'Latitude'/'Longitude'")
            # Try to use whatever columns are available
            print("   Attempting to use available columns...")
            # Return None to fall back to CSV or show error
            return None
        
        # Optimize memory usage - CRITICAL for Render free plan (512MB limit)
        print("💾 Optimizing memory usage...")
        
        # Convert string columns to category (saves 50-90% memory for repeated values)
        string_cols = ['offense', 'offense_sub_category', 'crime_against_category', 
                      'area', 'precinct', 'sector', 'time']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype('category')
        
        # Location column is too unique - keep as string but optimize
        # (can't use category for highly unique values)
        
        # Optimize numeric types
        if 'hour' in df.columns:
            df['hour'] = df['hour'].astype('int8')  # 0-23 fits in int8
        if 'hazardness' in df.columns:
            df['hazardness'] = pd.to_numeric(df['hazardness'], errors='coerce').astype('float32')
        if 'latitude' in df.columns:
            df['latitude'] = df['latitude'].astype('float32')
        if 'longitude' in df.columns:
            df['longitude'] = df['longitude'].astype('float32')
        
        # Check memory usage
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"✅ Successfully loaded {len(df)} records from Parquet")
        print(f"💾 DataFrame memory: {memory_mb:.2f} MB")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading Parquet: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def load_from_csv():
    """Load data from CSV file."""
    # Check if CSV file exists first
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV file not found: {CSV_FILE_PATH}")
        return None
    
    try:
        # Read CSV file
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"📊 Read {len(df)} rows from CSV")
        
        # Filter out records without coordinates
        df = df[df['Latitude'].notna() & df['Longitude'].notna()].copy()
        print(f"📍 After filtering for valid coordinates: {len(df)} records")
        
        # Create derived columns to match the expected schema
        # Build date from year/month/day columns
        df['date'] = pd.to_datetime(
            df['Offense Year'].astype(int).astype(str) + '-' + 
            df['Offense Month'].astype(int).astype(str).str.zfill(2) + '-' + 
            df['Offense Day'].astype(int).astype(str).str.zfill(2),
            format='%Y-%m-%d',
            errors='coerce'
        )
        
        # Time column
        df['time'] = df['Offense Time'].astype(str)
        
        # Extract hour from time
        df['hour'] = pd.to_datetime(df['Offense Time'], format='%H:%M:%S', errors='coerce').dt.hour
        # Fill missing hours with 0
        df['hour'] = df['hour'].fillna(0).astype(int)
        
        # Create datetime string
        df['datetime'] = df['date'].astype(str) + ' ' + df['Offense Time'].astype(str)
        
        # Rename columns to match expected schema
        df = df.rename(columns={
            'Offense Category': 'offense',
            'Offense Sub Category': 'offense_sub_category',
            'NIBRS Crime Against Category': 'crime_against_category',
            'Block Address': 'location',
            'Neighborhood': 'area',
            'Precinct': 'precinct',
            'Sector': 'sector',
            'Hazardness': 'hazardness',
            'Latitude': 'latitude',
            'Longitude': 'longitude'
        })
        
        # Select only the columns we need
        result = df[['date', 'time', 'hour', 'datetime', 'offense', 'offense_sub_category',
                     'crime_against_category', 'location', 'area', 'precinct', 'sector',
                     'hazardness', 'latitude', 'longitude']].copy()
        
        # Optimize memory usage by converting data types
        print("💾 Optimizing memory usage...")
        
        # Convert string columns to category (saves significant memory)
        string_cols = ['offense', 'offense_sub_category', 'crime_against_category', 
                      'location', 'area', 'precinct', 'sector', 'time']
        for col in string_cols:
            if col in result.columns:
                result[col] = result[col].astype('category')
        
        # Optimize numeric types
        if 'hour' in result.columns:
            result['hour'] = result['hour'].astype('int8')  # 0-23 fits in int8
        if 'hazardness' in result.columns:
            result['hazardness'] = pd.to_numeric(result['hazardness'], errors='coerce').astype('float32')
        if 'latitude' in result.columns:
            result['latitude'] = result['latitude'].astype('float32')
        if 'longitude' in result.columns:
            result['longitude'] = result['longitude'].astype('float32')
        
        # datetime column as string
        if 'datetime' in result.columns:
            result['datetime'] = result['datetime'].astype('string')
        
        memory_mb = result.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"✅ Successfully loaded {len(result)} records from CSV")
        print(f"💾 Memory usage: {memory_mb:.2f} MB")
        return result
        
    except Exception as e:
        print(f"❌ Error loading CSV: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def load_all_data(force_refresh=False):
    """
    Load and cache ALL data in memory from Parquet or CSV file.
    Prefers Parquet format for better performance, falls back to CSV.
    
    Args:
        force_refresh: If True, bypass cache and reload from source
        
    Returns:
        DataFrame with all crime data
    """
    global _data_cache, _cache_timestamp
    
    # Check if cache is still valid (data already loaded)
    if _data_cache is not None and not force_refresh:
        print(f"📦 Using cached data ({len(_data_cache)} records)")
        # Return copy only if force_refresh is needed, otherwise return view
        return _data_cache.copy() if force_refresh else _data_cache
    
    # Try Parquet first (preferred format - smaller and faster)
    print(f"🔍 Checking for data files...")
    print(f"   Parquet path: {PARQUET_FILE_PATH}")
    print(f"   CSV path: {CSV_FILE_PATH}")
    print(f"   Parquet exists: {os.path.exists(PARQUET_FILE_PATH)}")
    print(f"   CSV exists: {os.path.exists(CSV_FILE_PATH)}")
    
    if os.path.exists(PARQUET_FILE_PATH):
        print(f"🔄 Loading data from Parquet file...")
        df = load_from_parquet()
        if df is not None and not df.empty:
            result = df.copy()
            print(f"✅ Parquet load successful: {len(result)} records")
        else:
            print("⚠️  Parquet load failed or returned empty, falling back to CSV...")
            df = load_from_csv()
            if df is not None and not df.empty:
                result = df
                print(f"✅ CSV load successful: {len(result)} records")
            else:
                print("❌ Both Parquet and CSV loads failed")
                result = pd.DataFrame(columns=[
                    'date', 'time', 'hour', 'datetime', 'offense', 'offense_sub_category',
                    'crime_against_category', 'location', 'area', 'precinct', 'sector',
                    'hazardness', 'latitude', 'longitude'
                ])
    else:
        # Load from CSV if Parquet doesn't exist
        print(f"🔄 Parquet file not found, loading from CSV...")
        df = load_from_csv()
        if df is not None and not df.empty:
            result = df
            print(f"✅ CSV load successful: {len(result)} records")
        else:
            print("❌ CSV file not found or load failed.")
            print(f"   Checked paths:")
            print(f"   - Parquet: {PARQUET_FILE_PATH}")
            print(f"   - CSV: {CSV_FILE_PATH}")
            result = pd.DataFrame(columns=[
                'date', 'time', 'hour', 'datetime', 'offense', 'offense_sub_category',
                'crime_against_category', 'location', 'area', 'precinct', 'sector',
                'hazardness', 'latitude', 'longitude'
            ])
    
    # Cache the result
    _data_cache = result
    _cache_timestamp = time.time()
    
    print(f"✅ Loaded and cached {len(result)} records")
    
    return result.copy()

def get_date_range():
    """Get min and max date from cached data."""
    try:
        df = load_all_data()
        if not df.empty and 'date' in df.columns:
            return {'min': str(df['date'].min()), 'max': str(df['date'].max())}
    except Exception as e:
        print(f"Error getting date range: {e}")
    return {'min': '', 'max': ''}

# ---------------------------- Spatial Functions ----------------------------

def geojson_to_polygon_filter(geojson: dict) -> tuple:
    """
    Convert a Leaflet polygon GeoJSON to coordinate bounds for filtering.
    Returns (min_lat, max_lat, min_lon, max_lon) for simple bounding box filter.
    """
    coords = geojson["geometry"]["coordinates"][0]
    lats = [lat for lon, lat in coords]
    lons = [lon for lon, lat in coords]
    return (min(lats), max(lats), min(lons), max(lons))

def point_in_polygon(lat: float, lon: float, polygon_coords: list) -> bool:
    """
    Check if a point is inside a polygon using ray casting algorithm.
    polygon_coords: list of [lon, lat] pairs
    """
    x, y = lon, lat
    n = len(polygon_coords)
    inside = False
    
    p1x, p1y = polygon_coords[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon_coords[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    
    return inside

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance in meters between two points 
    on the earth (specified in decimal degrees).
    """
    from math import radians, cos, sin, asin, sqrt
    
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in meters
    r = 6371000
    
    return c * r


# ---------------------------- App ----------------------------

app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
app.title = "Seattle Crime Dashboard"
server = app.server  # Expose for gunicorn

# Get initial date range from DB (lazy load - will be loaded on first request)
# Don't load data at module import time to avoid blocking gunicorn startup
initial_date_range = {'min': '', 'max': ''}

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
                        "backgroundColor": "#1a1a1a" 
                    }
                ),

                html.H1(
                    "WatchDawg Crime Analytics",
                    className="text-center",
                    style={
                        "fontWeight": "700",
                        "fontSize": "2.4rem",
                        "color": "#B7A57A",  # Gold
                        "marginTop": "12px"
                    }
                ),

                html.P(
                    [
                        "Real-time incident visualization & statistical insights.",
                        html.Br(),
                        html.Span("By WatchDawg: Doyoung Jung, Wonjoon Hwang, Aneesh Singh, DH Lee, Jungmoon Ha, Jonathan Langley Grothe, Derek Tropf",
                            style={"fontWeight": "600", "color": "#8B7355"})  
                    ],
                    className="text-center",
                    style={"fontSize": "1.05rem", "color": "#bbb"}
                ),
                
                html.Hr(style={"borderColor": "#444", "margin": "20px 0"}),
                
                html.Div(
                    [
                        html.P(
                            [
                                html.I(className="bi bi-info-circle me-2"),
                                html.Strong("Data Source & Methodology", style={"color": "#B7A57A"})
                            ],
                            style={"fontSize": "0.95rem", "marginBottom": "8px"}
                        ),
                        html.P(
                            [
                                "This dashboard presents crime incident data from the ",
                                html.A(
                                    "Seattle Police Department",
                                    href="https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5/about_data",
                                    target="_blank",
                                    style={"color": "#B7A57A", "fontWeight": "bold", "textDecoration": "underline"}
                                ),
                                ", retrieved on December 4, 2025. All incidents are reported according to the ",
                                html.Strong("National Incident Based Reporting System (NIBRS)", style={"color": "#B7A57A"}),
                                ", a comprehensive standard that captures detailed contextual information for each crime, including precise location coordinates, crime classification, and temporal data. ",
                                "Neighborhood boundaries are defined using the official ",
                                html.Strong("City of Seattle Neighborhood Map", style={"color": "#B7A57A"}),
                                ", ensuring accurate geographic classification and analysis."
                            ],
                            style={"fontSize": "0.85rem", "lineHeight": "1.6", "color": "#999", "marginBottom": "0"}
                        )
                    ],
                    className="text-center",
                    style={"padding": "0 40px"}
                ),
            ],
            style={
                "textAlign": "center",
                "padding": "28px 20px",
                "border": "3px solid #B7A57A",  
                "borderRadius": "14px",
                "background": "rgba(30,30,30,0.95)",
                "backdropFilter": "blur(8px)",
                "boxShadow": "0 10px 28px rgba(0,0,0,0.5)"
            }
        ),
        html.Hr()
    ],
    style={"marginBottom": "24px"}
)

# Sticky left sidebar with collapsible date filter
sidebar = html.Div(
    id="sidebar-container",
    children=[
        html.Div(
            id="sidebar-content",
            children=[
                dbc.Card(
                    dbc.CardBody([
                        html.Div([
                            html.H5("📅 Date Filter", className="d-inline"),
                            dbc.Button(
                                "×",
                                id="collapse-button",
                                color="link",
                                className="float-end",
                                style={"fontSize": "24px", "color": "#B7A57A", "textDecoration": "none"}
                            ),
                        ], className="mb-3"),
                        html.Div([
                            html.Label([
                                html.I(className="bi bi-calendar-range me-1"),
                                "Date Range"
                            ], className="fw-semibold mb-2 d-block", style={"fontSize": "0.9rem"}),
                            dmc.MantineProvider(
                                theme={"colorScheme": "dark"},
                                children=dmc.DatePickerInput(
                                    id="date-range-picker-mantine",
                                    type="range",
                                    label="",
                                    placeholder="Select date range",
                                    value=[
                                        (pd.Timestamp.now() - pd.Timedelta(days=365)).strftime('%Y-%m-%d'),
                                        pd.Timestamp.now().strftime('%Y-%m-%d')
                                    ],
                                    valueFormat="MMM D, YYYY",
                                    clearable=True,
                                    style={"width": "100%"},
                                    popoverProps={"zIndex": 10000, "styles": {"dropdown": {"backgroundColor": "white"}}},
                                    styles={
                                        "calendarHeader": {"color": "black"},
                                        "calendarHeaderControl": {"color": "black"},
                                        "calendarHeaderLevel": {"color": "black"},
                                        "day": {"color": "black"},
                                        "weekday": {"color": "black"},
                                        "month": {"color": "black"}
                                    }
                                )
                            ),
                            # Hidden store to maintain compatibility with existing callbacks
                            dcc.Store(id="date-range-picker", data={
                                "start_date": (pd.Timestamp.now() - pd.Timedelta(days=365)).strftime('%Y-%m-%d'),
                                "end_date": pd.Timestamp.now().strftime('%Y-%m-%d')
                            }),
                            html.Small(id="date-range-info", className="text-muted d-block mt-1", style={"fontSize": "9px"})
                        ], className="mb-3"),
                        html.Hr(),
                        html.Label("📊 Data Source", className="fw-semibold mb-2"),
                        html.Small(id="cache-info", className="text-muted d-block mb-2"),
                        dcc.Store(id="refresh-trigger", data=0)
                    ]),
                    style={"backgroundColor": "#2b2b2b"}
                )
            ],
            style={
                "position": "fixed",
                "top": "20px",
                "left": "20px",
                "width": "280px",
                "zIndex": "9999",
                "maxHeight": "90vh",
                "overflowY": "auto"
            }
        ),
        # Collapsed state - just a small button
        html.Div(
            id="sidebar-collapsed",
            children=[
                dbc.Button(
                    "📅",
                    id="expand-button",
                    color="primary",
                    className="btn-sm",
                    style={"fontSize": "20px", "padding": "10px 15px"}
                ),
            ],
            style={
                "position": "fixed",
                "top": "20px",
                "left": "20px",
                "zIndex": "9999",
                "display": "none"  # Hidden by default
            }
        )
    ]
)

# Date range display (shown above KPIs)
date_range_display = html.Div(
    id="date-range-display",
    className="text-center text-muted mb-3",
    style={"fontSize": "13px"}
)

# KPI cards
kpi_cards = dbc.Row(
    [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Total Incidents", className="text-muted"),
                    dcc.Loading(
                        id="loading-kpi-total",
                        type="circle",
                        children=html.H2(id="kpi-total", className="mb-0 text-light")
                    )
                ]),
                className="text-center",
                style={"backgroundColor": "#2b2b2b"}
            ),
            md=3
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6([
                        "Against Property ",
                        html.Span("ⓘ", id="property-info-icon", style={"cursor": "pointer", "fontSize": "12px"})
                    ], className="text-muted"),
                    dbc.Tooltip(
                        "Crimes targeting property or belongings: theft, burglary, motor vehicle theft, "
                        "vandalism, arson, shoplifting, and fraud.",
                        target="property-info-icon",
                        placement="bottom",
                        style={"maxWidth": "280px"}
                    ),
                    dcc.Loading(
                        id="loading-kpi-property",
                        type="circle",
                        children=[
                            html.H2(id="kpi-property", className="mb-0 text-warning")
                        ]
                    )
                ]),
                className="text-center",
                style={"backgroundColor": "#2b2b2b"}
            ),
            md=3
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6([
                        "Against Person ",
                        html.Span("ⓘ", id="person-info-icon", style={"cursor": "pointer", "fontSize": "12px"})
                    ], className="text-muted"),
                    dbc.Tooltip(
                        "Crimes with a direct victim: assault, robbery, homicide, kidnapping, "
                        "sexual offenses, and intimidation/threats.",
                        target="person-info-icon",
                        placement="bottom",
                        style={"maxWidth": "280px"}
                    ),
                    dcc.Loading(
                        id="loading-kpi-person",
                        type="circle",
                        children=[
                            html.H2(id="kpi-person", className="mb-0 text-danger")
                        ]
                    )
                ]),
                className="text-center",
                style={"backgroundColor": "#2b2b2b"}
            ),
            md=3
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.H6([
                        "Against Society ",
                        html.Span("ⓘ", id="society-info-icon", style={"cursor": "pointer", "fontSize": "12px"})
                    ], className="text-muted"),
                    dbc.Tooltip(
                        "Crimes against the public order with no direct victim: drug/narcotic offenses, "
                        "weapons violations, prostitution, gambling, and trespassing.",
                        target="society-info-icon",
                        placement="bottom",
                        style={"maxWidth": "280px"}
                    ),
                    dcc.Loading(
                        id="loading-kpi-society",
                        type="circle",
                        children=[
                            html.H2(id="kpi-society", className="mb-0 text-info")
                        ]
                    )
                ]),
                className="text-center",
                style={"backgroundColor": "#2b2b2b"}
            ),
            md=3
        ),
    ],
    className="g-3 mb-3"
)

# Charts row - top row with trend line and bar chart
trend_row = dbc.Row(
    [
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.Div([
                        html.H5("Crime Trends by Neighborhood", className="card-title mb-0"),
                        html.Div([
                            # Metric toggle
                            html.Span("Metric:", className="me-2 text-muted small"),
                            dbc.Button(
                                "Count", 
                                id="metric-count-btn", 
                                size="sm", 
                                outline=False,
                                style={"borderColor": "#6c757d", "backgroundColor": "#6c757d", "color": "white"},
                                className="me-1"
                            ),
                            dbc.Button(
                                "Hazard Score", 
                                id="metric-hazard-btn", 
                                size="sm", 
                                outline=True,
                                style={"borderColor": "#6c757d", "color": "#6c757d"},
                                className="me-1"
                            ),
                            html.Span(
                                "ⓘ",
                                id="hazard-info-icon",
                                style={
                                    "cursor": "pointer",
                                    "color": "#6c757d",
                                    "fontSize": "14px",
                                    "marginRight": "12px"
                                }
                            ),
                            dbc.Tooltip(
                                "Hazard Score is an opinionated metric that weighs more dangerous crimes "
                                "(e.g., assault, shootings) with higher scores, and societal crimes "
                                "(e.g., narcotics, trespassing) with lower scores.",
                                target="hazard-info-icon",
                                placement="bottom",
                                style={"maxWidth": "300px"}
                            ),
                            # Sort toggle
                            html.Span("Sort:", className="me-2 text-muted small"),
                            dbc.Button(
                                "Lowest", 
                                id="bottom-neighborhoods-btn", 
                                size="sm", 
                                outline=True,
                                style={"borderColor": "#28a745", "color": "#28a745"},
                                className="me-1"
                            ),
                            dbc.Button(
                                "Highest", 
                                id="top-neighborhoods-btn", 
                                size="sm", 
                                outline=False,
                                style={"borderColor": "#dc3545", "backgroundColor": "#dc3545", "color": "white"},
                            )
                        ], className="d-flex align-items-center")
                    ], className="d-flex justify-content-between align-items-center mb-2"),
                    html.P(id="line-chart-subtitle", className="text-muted small mb-3"),
                    dcc.Loading(
                        id="loading-line-chart",
                        type="default",
                        children=dcc.Graph(id="line-trends")
                    ),
                    dcc.Store(id="neighborhood-sort-order", data="top"),  # "top" or "bottom"
                    dcc.Store(id="line-chart-metric", data="count")  # "count" or "hazard"
                ])
            ),
            md=6
        ),
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.Div([
                        html.H5("Crime Count by Type & Category", className="card-title d-inline"),
                        html.Div([
                            html.Button("← Back", id="drill-back-btn", className="btn btn-sm btn-outline-warning me-2", style={"display": "none"})
                        ], className="d-inline")
                    ], className="d-flex justify-content-between align-items-center"),
                    html.P(id="bar-chart-subtitle", children="Click on a category to drill down", className="text-muted small"),
                    dcc.Loading(
                        id="loading-bar-chart",
                        type="default",
                        children=dcc.Graph(id="bar-crime-types", clear_on_unhover=True)
                    ),
                    dcc.Store(id="drill-level-store", data="category"),  # 'category' or 'subcategory'
                    dcc.Store(id="selected-category-store", data=None),  # Store which category was clicked
                    dcc.Store(id="click-reset-counter", data=0)  # Counter to force callback refresh
                ])
            ),
            md=6
        ),
    ],
    className="g-3 mb-3"
)

# Charts row removed - pie and bar charts no longer displayed

# Crime Map Card (point-based visualization)
map_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("🗺️ Crime Location Map", className="card-title"),
            html.P(
                "Each dot represents a crime incident. Use the filters on the right to explore the data.",
                className="text-muted mb-2"
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.Label("📍 Search Address & Draw Circle", className="fw-semibold mb-2"),
                                    dbc.InputGroup([
                                        dbc.Input(
                                            id="address-input",
                                            placeholder="Enter Seattle address (e.g., 400 Broad St, Seattle, WA)",
                                            type="text"
                                        ),
                                        dbc.Button("Search", id="address-search-btn", color="primary", n_clicks=0),
                                        dbc.Button("Reset", id="address-reset-btn", color="danger", n_clicks=0)
                                    ], className="mb-2"),
                                    html.Label("Circle Radius", className="small fw-semibold mb-1"),
                                    dcc.Slider(
                                        id="circle-radius-input",
                                        min=161,
                                        max=3219,
                                        step=80,
                                        value=805,
                                        marks={
                                            161: {"label": "0.1 mi", "style": {"fontSize": "11px"}},
                                            805: {"label": "0.5 mi", "style": {"fontSize": "11px"}},
                                            1609: {"label": "1 mi", "style": {"fontSize": "11px"}},
                                            2414: {"label": "1.5 mi", "style": {"fontSize": "11px"}},
                                            3219: {"label": "2 mi", "style": {"fontSize": "11px"}}
                                        },
                                        tooltip={"always_visible": False, "placement": "bottom"},
                                        className="mb-2"
                                    ),
                                    html.Div(id="radius-display", className="text-center text-muted small mb-2"),
                                    html.Div(id="address-status", className="text-muted small mb-3"),
                                ],
                                className="mb-3",
                                style={"paddingTop": "10px", "paddingBottom": "10px"}
                            ),
                            dcc.Graph(
                                id="crime-map",
                                style={"height": "650px", "width": "100%"},
                                config={
                                    "scrollZoom": True,
                                    "modeBarButtonsToRemove": ["select2d", "lasso2d", "select", "lasso"]
                                }
                            ),
                            html.Div([
                                html.Small(id="map-stats", className="text-muted", style={"fontSize": "11px"})
                            ], className="mt-2"),
                            # Hidden stores for compatibility
                            dcc.Store(id="address-circle-layer"),
                            dcc.Store(id="points-layer")
                        ],
                        md=9
                    ),
                    dbc.Col(
                        [
                            # Neighborhood Filter FIRST
                            html.Label("🏘️ Neighborhood Filter", className="fw-semibold mb-2"),
                            dcc.Loading(
                                id="loading-neighborhood-filter",
                                type="circle",
                                children=dcc.Dropdown(
                                id="neighborhood-filter",
                                    options=[],  # Will be populated by callback
                                value=[],
                                    placeholder="Loading neighborhoods...",
                                multi=True,
                                clearable=True,
                                className="mb-3"
                                )
                            ),
                            html.Hr(),
                            # Time Filter SECOND
                            html.Label("⏰ Time Filter", className="fw-semibold mb-2 text-center", style={"display": "block"}),
                            html.Small("Select hour range", className="text-muted d-block mb-2 text-center", style={"fontSize": "11px"}),
                            # Later button on TOP (moves time window later/up)
                            html.Div(
                                dbc.Button("▲", id="shift-later-btn", color="info", size="sm", style={"fontSize": "14px", "padding": "2px 12px"}),
                                className="text-center mb-2"
                            ),
                            html.Div([
                                dcc.RangeSlider(
                                    id="hour-range-slider",
                                    min=0,
                                    max=24,
                                    step=1,
                                    value=[0, 24],  # Default: all hours
                                    marks={
                                        0: {"label": "12am", "style": {"color": "#aaa"}},
                                        6: {"label": "6am", "style": {"color": "#aaa"}},
                                        12: {"label": "12pm", "style": {"color": "#aaa"}},
                                        18: {"label": "6pm", "style": {"color": "#aaa"}},
                                        24: {"label": "12am", "style": {"color": "#aaa"}}
                                    },
                                    vertical=True,
                                    verticalHeight=330,
                                    tooltip={"placement": "right", "always_visible": False},
                                    allowCross=False
                                )
                            ], style={"height": "360px", "display": "flex", "justifyContent": "center", "marginBottom": "5px"}),
                            # Earlier button on BOTTOM (moves time window earlier/down)
                            html.Div(
                                dbc.Button("▼", id="shift-earlier-btn", color="info", size="sm", style={"fontSize": "14px", "padding": "2px 12px"}),
                                className="text-center mb-2"
                            ),
                            dbc.Button("Reset", id="reset-hour-btn", color="warning", size="sm", className="mb-3 w-100"),
                            dcc.Store(id="hour-slider", data=[0, 24]),  # Store for hour range
                            html.Div(id="clock-selector", style={"display": "none"}),  # Hidden for compatibility
                            html.Hr(),
                            # Crime Type Filter THIRD
                            html.Label([
                                "🗺️ Crime Type Filter ",
                                html.Span("ⓘ", id="crime-type-info-icon", style={"cursor": "pointer", "fontSize": "12px", "color": "#6c757d"})
                            ], className="fw-semibold mb-2"),
                            dbc.Tooltip(
                                [
                                    html.Div([
                                        html.Strong("Person", style={"color": "#dc3545"}),
                                        html.Span(": Crimes with a direct victim (assault, robbery, homicide)")
                                    ], className="mb-1"),
                                    html.Div([
                                        html.Strong("Property", style={"color": "#ffc107"}),
                                        html.Span(": Crimes targeting belongings (theft, burglary, vandalism)")
                                    ], className="mb-1"),
                                    html.Div([
                                        html.Strong("Society", style={"color": "#17a2b8"}),
                                        html.Span(": Crimes against public order (drugs, weapons, trespassing)")
                                    ])
                                ],
                                target="crime-type-info-icon",
                                placement="left",
                                style={"maxWidth": "320px", "textAlign": "left"}
                            ),
                            dcc.Loading(
                                id="loading-category-filter",
                                type="circle",
                                children=dcc.Checklist(
                                id="category-filter",
                                    options=[],  # Will be populated by callback
                                    value=[],
                                    className="mb-3",
                                    labelStyle={"display": "block", "marginBottom": "8px"},
                                    inputStyle={"marginRight": "8px"}
                                )
                            )
                        ],
                        md=3
                    ),
                ],
                className="g-3 align-items-start flex-column flex-md-row"
            ),
            dcc.Store(id="poly-store", data=None),  # Store polygon GeoJSON
            dcc.Store(id="address-coords-store", data=None),  # Store geocoded address coordinates
            html.Hr(),
            html.H6("📋 Crime Details (top 500)", className="text-light mb-3"),
            dcc.Loading(
                id="loading-table",
                type="default",
                children=dash_table.DataTable(
                id="details-table",
                columns=[
                        {"name": "Date & Time", "id": "Date & Time"},
                    {"name": "Offense Type", "id": "Offense Type"},
                    {"name": "Crime Against", "id": "Crime Against"},
                    {"name": "Location", "id": "location"},
                    {"name": "Area", "id": "area"},
                    {"name": "Hazard Score", "id": "Hazard Score"},
                ],
                data=[],
                page_size=20,
                page_current=0,
                style_table={
                    "overflowX": "auto",
                    "backgroundColor": "#1a1a1a"
                },
                style_cell={
                    "whiteSpace": "normal",
                    "height": "auto",
                    "textAlign": "left",
                    "backgroundColor": "#2b2b2b",
                    "color": "#e0e0e0",
                    "border": "1px solid #444",
                    "padding": "10px",
                    "fontSize": "14px"
                },
                style_header={
                    "backgroundColor": "#1a1a1a",
                    "color": "#B7A57A",
                    "fontWeight": "bold",
                    "border": "1px solid #444",
                    "textAlign": "left",
                    "padding": "12px",
                    "fontSize": "14px"
                },
                style_data_conditional=[
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "#252525"
                    },
                    {
                        "if": {"state": "selected"},
                        "backgroundColor": "#3a3a3a",
                        "border": "1px solid #B7A57A"
                    }
                ],
                style_as_list_view=False,
                page_action='native',
                sort_action='native',
                filter_action='native'
                )
            )
        ]
    ),
    className="mb-4"
)

app.layout = dbc.Container(
    [
        sidebar,
        html.Div(
            id="main-content",
            children=[
        header,
                date_range_display,  # Shows "Data shown for X to Y"
        kpi_cards,
        trend_row,
                map_card,  # Point-based crime map
            ],
            style={"marginLeft": "320px", "transition": "margin-left 0.3s ease"}  # Leave space for fixed sidebar
        )
    ],
    fluid=True,
    style={"paddingLeft": "0px"}
)


# ---------------------------- Callbacks ----------------------------

# Display cache information
@app.callback(
    Output("cache-info", "children"),
    Input("date-range-picker", "id"),  # Dummy input to trigger on load
    Input("refresh-trigger", "data"),
    prevent_initial_call=False
)
def display_cache_info(_, refresh_count):
    """Display information about the data source."""
    # Try to load data if cache is empty
    if _data_cache is None or len(_data_cache) == 0:
        print("⚠️  Cache is empty, attempting to load data...")
        try:
            df = load_all_data()
            if df is not None and len(df) > 0:
                record_count = len(df)
                source = "Parquet" if os.path.exists(PARQUET_FILE_PATH) else "CSV"
                return [
                    html.Div(f"📊 {record_count:,} records", className="small"),
                    html.Div(f"📁 Source: crime_data_gold.{source.lower()}", className="small")
                ]
        except Exception as e:
            print(f"❌ Error loading data in display_cache_info: {e}")
            import traceback
            traceback.print_exc()
        
        # Check which files exist
        parquet_exists = os.path.exists(PARQUET_FILE_PATH)
        csv_exists = os.path.exists(CSV_FILE_PATH)
        
        if not parquet_exists and not csv_exists:
            return html.Div([
                html.I(className="bi bi-exclamation-triangle text-warning me-1"),
                "No data file found - check Parquet or CSV file"
            ], className="small text-warning")
        else:
            return html.Div([
                html.I(className="bi bi-exclamation-triangle text-warning me-1"),
                "Data loading failed - check logs"
            ], className="small text-warning")
    
    record_count = len(_data_cache) if _data_cache is not None else 0
    source = "Parquet" if os.path.exists(PARQUET_FILE_PATH) else "CSV"
    
    return [
        html.Div(f"📊 {record_count:,} records", className="small"),
        html.Div(f"📁 Source: crime_data_gold.{source.lower()}", className="small")
    ]

# Toggle sidebar collapse/expand
@app.callback(
    Output("sidebar-content", "style"),
    Output("sidebar-collapsed", "style"),
    Output("main-content", "style"),
    Input("collapse-button", "n_clicks"),
    Input("expand-button", "n_clicks"),
    prevent_initial_call=True
)
def toggle_sidebar(collapse_clicks, expand_clicks):
    """Toggle the entire sidebar visibility."""
    from dash import callback_context
    
    # Determine which button was clicked
    if not callback_context.triggered:
        # Default state - sidebar open
        return (
            {"position": "fixed", "top": "20px", "left": "20px", "width": "280px", "zIndex": "1000", "maxHeight": "90vh", "overflowY": "auto"},
            {"position": "fixed", "top": "20px", "left": "20px", "zIndex": "1000", "display": "none"},
            {"marginLeft": "320px", "transition": "margin-left 0.3s ease"}
        )
    
    trigger_id = callback_context.triggered[0]['prop_id'].split('.')[0]
    
    if trigger_id == "collapse-button":
        # Collapse sidebar - hide content, show button
        return (
            {"display": "none"},
            {"position": "fixed", "top": "20px", "left": "20px", "zIndex": "1000", "display": "block"},
            {"marginLeft": "0px", "transition": "margin-left 0.3s ease"}
        )
    else:  # expand-button
        # Expand sidebar - show content, hide button
        return (
            {"position": "fixed", "top": "20px", "left": "20px", "width": "280px", "zIndex": "1000", "maxHeight": "90vh", "overflowY": "auto"},
            {"position": "fixed", "top": "20px", "left": "20px", "zIndex": "1000", "display": "none"},
            {"marginLeft": "320px", "transition": "margin-left 0.3s ease"}
        )

# Dynamically populate category filter based on actual data
@app.callback(
    Output("category-filter", "options"),
    Output("category-filter", "value"),
    Input("category-filter", "id"),
    prevent_initial_call=False
)
def init_category_filter(_):
    """Initialize category filter checklist with actual categories from data."""
    try:
        df = load_all_data()
        
        # Labels and colors matching the bar chart
        category_info = {
            "PERSON": {"label": "Person", "color": "#dc3545"},      # Red
            "PROPERTY": {"label": "Property", "color": "#ffc107"},  # Yellow/Gold
            "SOCIETY": {"label": "Society", "color": "#17a2b8"},    # Teal
            "ANY": {"label": "Any", "color": "#9b59b6"},
            "NOT_A_CRIME": {"label": "Not a Crime", "color": "#6c757d"}
        }
        
        # Get actual categories from data
        if not df.empty and 'crime_against_category' in df.columns:
            actual_categories = sorted(df['crime_against_category'].dropna().unique().tolist())
        else:
            actual_categories = ["PERSON", "PROPERTY", "SOCIETY"]
        
        # Build options with colored dots
        options = []
        for cat in actual_categories:
            info = category_info.get(cat, {"label": cat, "color": "#6c757d"})
            # Create label with colored circle
            label = html.Span([
                html.Span("●", style={"color": info["color"], "fontSize": "16px", "marginRight": "8px"}),
                info["label"]
            ])
            options.append({"label": label, "value": cat})
        
        # Default: select all actual categories
        default_selection = actual_categories
        
        print(f"DEBUG INIT: Initializing category filter with: {actual_categories}")
        
        return options, default_selection
        
    except Exception as e:
        print(f"Error initializing category filter: {e}")
        import traceback
        traceback.print_exc()
        # Fallback
        return [], []

# Category filter sync removed - using dropdown value directly

# Initialize neighborhood filter with actual data
@app.callback(
    Output("neighborhood-filter", "options"),
    Output("neighborhood-filter", "placeholder"),
    Input("neighborhood-filter", "id"),
    prevent_initial_call=False
)
def init_neighborhood_filter(_):
    """Load actual neighborhoods from data."""
    try:
        df = load_all_data()
        
        if df.empty or 'area' not in df.columns:
            return [], "No neighborhoods available"
        
        # Get sorted list of neighborhoods
        neighborhoods = sorted(df['area'].dropna().unique().tolist())
        
        if not neighborhoods:
            return [], "No neighborhoods available"
        
        print(f"📍 Loaded {len(neighborhoods)} neighborhoods from data")
        
        # Create options
        options = [{"label": n, "value": n} for n in neighborhoods]
        
        return options, "Select neighborhood(s)"
        
    except Exception as e:
        print(f"Error loading neighborhoods: {e}")
        import traceback
        traceback.print_exc()
        return [], "Error loading neighborhoods"

# Display date range info
@app.callback(
    Output("date-range-info", "children"),
    Input("date-range-picker", "data"),
    prevent_initial_call=False
)
def display_date_info(date_data):
    """Display information about the selected date range."""
    if not date_data or not date_data.get("start_date") or not date_data.get("end_date"):
        return "Loading..."
    start_date = date_data["start_date"]
    end_date = date_data["end_date"]
    
    try:
        df = load_all_data()
        df['date'] = pd.to_datetime(df['date'])
        
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        filtered_count = len(df[(df['date'] >= start) & (df['date'] <= end)])
        total_count = len(df)
        
        days_diff = (end - start).days
        
        return f"📊 {filtered_count:,} incidents ({filtered_count/total_count*100:.1f}% of total) • {days_diff} days selected"
    except Exception as e:
        return f"Error: {str(e)}"

# Sync Mantine DatePickerInput to the store
@app.callback(
    Output("date-range-picker", "data"),
    Input("date-range-picker-mantine", "value"),
    prevent_initial_call=False
)
def sync_date_picker(mantine_value):
    """Sync Mantine DatePickerInput value to the store for compatibility."""
    if mantine_value and len(mantine_value) == 2 and mantine_value[0] and mantine_value[1]:
        return {"start_date": mantine_value[0], "end_date": mantine_value[1]}
    # Default to last year
    return {
        "start_date": (pd.Timestamp.now() - pd.Timedelta(days=365)).strftime('%Y-%m-%d'),
        "end_date": pd.Timestamp.now().strftime('%Y-%m-%d')
    }

# Update date range display text
@app.callback(
    Output("date-range-display", "children"),
    Input("date-range-picker", "data"),
    prevent_initial_call=False
)
def update_date_range_display(date_data):
    """Show the current date range being used for KPIs, line chart, and bar chart."""
    if date_data and date_data.get("start_date") and date_data.get("end_date"):
        start = pd.to_datetime(date_data["start_date"])
        end = pd.to_datetime(date_data["end_date"])
        start_str = start.strftime("%b %d, %Y")
        end_str = end.strftime("%b %d, %Y")
        return f"📅 Data shown for {start_str} to {end_str}"
    return ""

# Display current radius value
@app.callback(
    Output("radius-display", "children"),
    Input("circle-radius-input", "value"),
    prevent_initial_call=False
)
def display_radius(radius):
    """Display the current radius value."""
    if radius:
        miles = radius / 1609.34
        return f"Radius: {miles:.2f} miles ({radius:.0f} meters)"
    return "Radius: 0.5 miles (805 meters)"

# Sync range slider to hour-slider store
@app.callback(
    Output("hour-slider", "data"),
    Input("hour-range-slider", "value"),
    prevent_initial_call=False
)
def sync_hour_range(range_value):
    """Sync the range slider value to the store."""
    if range_value is None:
        return [0, 24]
    return range_value

# Hidden clock-selector (kept for compatibility)
@app.callback(
    Output("clock-selector", "children"),
    Input("hour-slider", "data"),
    prevent_initial_call=False
)
def render_hour_donut(current_range):
    """Hidden - kept for compatibility."""
    return None  # Hidden element

# Handle reset button for hour range
@app.callback(
    Output("hour-range-slider", "value"),
    Input("reset-hour-btn", "n_clicks"),
    prevent_initial_call=True
)
def handle_reset_hour(n_clicks):
    """Handle reset button to show all hours."""
    return [0, 24]

# Shift time range earlier (both handles move up by 1 hour)
@app.callback(
    Output("hour-range-slider", "value", allow_duplicate=True),
    Input("shift-earlier-btn", "n_clicks"),
    State("hour-range-slider", "value"),
    prevent_initial_call=True
)
def shift_earlier(n_clicks, current_range):
    """Shift the entire time range 1 hour earlier."""
    if current_range and len(current_range) == 2:
        start, end = current_range
        if start > 0:  # Can shift earlier
            return [start - 1, end - 1]
    return no_update

# Shift time range later (both handles move down by 1 hour)
@app.callback(
    Output("hour-range-slider", "value", allow_duplicate=True),
    Input("shift-later-btn", "n_clicks"),
    State("hour-range-slider", "value"),
    prevent_initial_call=True
)
def shift_later(n_clicks, current_range):
    """Shift the entire time range 1 hour later."""
    if current_range and len(current_range) == 2:
        start, end = current_range
        if end < 24:  # Can shift later
            return [start + 1, end + 1]
    return no_update

# Reset circle
@app.callback(
    Output("address-coords-store", "data", allow_duplicate=True),
    Output("address-input", "value"),
    Output("address-status", "children", allow_duplicate=True),
    Input("address-reset-btn", "n_clicks"),
    prevent_initial_call=True
)
def reset_circle(n_clicks):
    """Reset the circle and clear the address input."""
    return None, "", ""

# Store for the last searched location
@app.callback(
    Output("address-coords-store", "data", allow_duplicate=True),
    Input("address-search-btn", "n_clicks"),
    State("address-input", "value"),
    prevent_initial_call=True
)
def store_address_coords(n_clicks, address):
    """Store the geocoded coordinates."""
    if not address:
        return None
    
    try:
        import urllib.parse
        import urllib.request
        import json
        
        if "seattle" not in address.lower():
            address = f"{address}, Seattle, WA"
        
        encoded_address = urllib.parse.quote(address)
        url = f"https://nominatim.openstreetmap.org/search?q={encoded_address}&format=json&limit=1"
        req = urllib.request.Request(url, headers={'User-Agent': 'Seattle Crime Dashboard'})
        
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
        
        if data:
            return {
                'lat': float(data[0]['lat']),
                'lon': float(data[0]['lon']),
                'display_name': data[0]['display_name']
            }
    except Exception as e:
        print(f"Geocoding error: {e}")
    
    return None

# Update circle status when radius changes or address is searched
@app.callback(
    Output("address-circle-layer", "data"),
    Output("address-status", "children"),
    Input("address-coords-store", "data"),
    Input("circle-radius-input", "value"),
    prevent_initial_call=False
)
def update_circle(coords, radius):
    """Update the circle data based on stored coordinates and current radius."""
    if not coords:
        return None, ""
    
    lat = coords['lat']
    lon = coords['lon']
    display_name = coords['display_name']
    
    # Ensure radius has a valid value (default: 0.5 miles = 805 meters)
    if not radius:
        radius = 805
    
    # Store circle data for the map callback to use
    circle_data = {
        'lat': lat,
        'lon': lon,
        'radius': radius,
        'display_name': display_name
    }
    
    miles = radius / 1609.34
    status = f"📍 {display_name[:50]}... | Radius: {miles:.2f} mi"
    
    return circle_data, status

# Handle top/bottom neighborhood toggle
@app.callback(
    Output("neighborhood-sort-order", "data"),
    Output("top-neighborhoods-btn", "style"),
    Output("bottom-neighborhoods-btn", "style"),
    Input("top-neighborhoods-btn", "n_clicks"),
    Input("bottom-neighborhoods-btn", "n_clicks"),
    prevent_initial_call=False
)
def toggle_neighborhood_sort(top_clicks, bottom_clicks):
    """Toggle between top and bottom neighborhoods."""
    from dash import callback_context
    
    # Styles for selected/unselected states
    red_selected = {"borderColor": "#dc3545", "backgroundColor": "#dc3545", "color": "white"}
    red_unselected = {"borderColor": "#dc3545", "backgroundColor": "transparent", "color": "#dc3545"}
    green_selected = {"borderColor": "#28a745", "backgroundColor": "#28a745", "color": "white"}
    green_unselected = {"borderColor": "#28a745", "backgroundColor": "transparent", "color": "#28a745"}
    
    if not callback_context.triggered:
        return "top", red_selected, green_unselected  # Default to top (highest crime)
    
    button_id = callback_context.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == "top-neighborhoods-btn":
        return "top", red_selected, green_unselected  # Highest Crime selected
    elif button_id == "bottom-neighborhoods-btn":
        return "bottom", red_unselected, green_selected  # Lowest Crime selected
    
    return "top", red_selected, green_unselected

# Handle metric toggle (count vs hazard score)
@app.callback(
    Output("line-chart-metric", "data"),
    Output("metric-count-btn", "style"),
    Output("metric-hazard-btn", "style"),
    Input("metric-count-btn", "n_clicks"),
    Input("metric-hazard-btn", "n_clicks"),
    prevent_initial_call=False
)
def toggle_line_chart_metric(count_clicks, hazard_clicks):
    """Toggle between crime count and hazard score metrics."""
    from dash import callback_context
    
    # Styles for selected/unselected states
    gray_selected = {"borderColor": "#6c757d", "backgroundColor": "#6c757d", "color": "white"}
    gray_unselected = {"borderColor": "#6c757d", "backgroundColor": "transparent", "color": "#6c757d"}
    
    if not callback_context.triggered:
        return "count", gray_selected, gray_unselected  # Default to count
    
    button_id = callback_context.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == "metric-count-btn":
        return "count", gray_selected, gray_unselected
    elif button_id == "metric-hazard-btn":
        return "hazard", gray_unselected, gray_selected
    
    return "count", gray_selected, gray_unselected

# Update line chart subtitle based on sort order and metric
@app.callback(
    Output("line-chart-subtitle", "children"),
    Input("neighborhood-sort-order", "data"),
    Input("line-chart-metric", "data"),
    prevent_initial_call=False
)
def update_line_chart_subtitle(sort_order, metric):
    """Update subtitle based on selected sort order and metric."""
    metric_text = "avg hazard score" if metric == "hazard" else "crime count"
    safety_text = "lowest hazard" if metric == "hazard" else "safest"
    danger_text = "highest hazard" if metric == "hazard" else "highest crime"
    if sort_order == "bottom":
        return f"Bottom 10 neighborhoods by {metric_text} ({safety_text} areas)"
    else:
        return f"Top 10 neighborhoods by {metric_text} ({danger_text} areas)"

# Update trend line chart
@app.callback(
    Output("line-trends", "figure"),
    Input("date-range-picker", "data"),
    Input("neighborhood-sort-order", "data"),
    Input("line-chart-metric", "data"),
    prevent_initial_call=False
)
def update_trend_chart(date_data, sort_order, metric):
    """Update the crime trends by neighborhood line chart with date filter."""
    try:
        start_date = date_data.get("start_date") if date_data else None
        end_date = date_data.get("end_date") if date_data else None
        
        # Default to "top" if sort_order is None
        if sort_order is None:
            sort_order = "top"
        
        # Default to "count" if metric is None
        if metric is None:
            metric = "count"
        
        # Get data and apply date filter
        df = load_all_data()
        if start_date and end_date and not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
        
        # Exclude current incomplete month - only show up to end of last full month
        if not df.empty:
            from datetime import datetime
            today = datetime.now()
            # Get the first day of current month
            first_of_current_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Filter to only include data before the current month
            df = df[df['date'] < first_of_current_month].copy()
        
        # Get top 10 neighborhoods from filtered data
        if df.empty or 'area' not in df.columns:
            df_trends = pd.DataFrame(columns=['date', 'area', 'value'])
        else:
            # Ensure date is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Add week/month aggregation based on date range
            date_range_days = (df['date'].max() - df['date'].min()).days
            
            if date_range_days > 180:  # More than 6 months - aggregate by month
                df['period'] = df['date'].dt.to_period('M').dt.to_timestamp()
                x_label = 'Month'
            elif date_range_days > 60:  # 2-6 months - aggregate by week
                df['period'] = df['date'].dt.to_period('W').dt.to_timestamp()
                x_label = 'Week'
            else:  # Less than 2 months - keep daily
                df['period'] = df['date']
                x_label = 'Date'
            
            # Determine ranking based on metric
            if metric == "hazard":
                # Ensure hazardness column exists and is numeric
                if 'hazardness' in df.columns:
                    df['hazardness'] = pd.to_numeric(df['hazardness'], errors='coerce').fillna(0)
                    # Rank by average hazard score
                    area_metric = df.groupby('area')['hazardness'].mean().sort_values(ascending=False)
                else:
                    area_metric = df['area'].value_counts()
            else:
                area_metric = df['area'].value_counts()
            
            # Get top 10 or bottom 10 based on sort_order
            if sort_order == "bottom":
                # Get bottom 10 (safest neighborhoods)
                selected_areas = area_metric.tail(10).index.tolist()
            else:
                # Get top 10 (highest crime/hazard neighborhoods) - default
                selected_areas = area_metric.head(10).index.tolist()
            
            # Aggregate based on metric
            if metric == "hazard" and 'hazardness' in df.columns:
                # Use average hazard score per period
                df_trends = df[df['area'].isin(selected_areas)].groupby(['period', 'area'])['hazardness'].mean().reset_index(name='value')
            else:
                df_trends = df[df['area'].isin(selected_areas)].groupby(['period', 'area']).size().reset_index(name='value')
            
            df_trends = df_trends.rename(columns={'period': 'date'})
            df_trends = df_trends.sort_values('date')
        
        # Set y-axis label based on metric
        y_label = "Avg Hazard Score" if metric == "hazard" else "Number of Crimes"
        
        if df_trends.empty:
            fig = px.line()
            fig.update_layout(
                height=350,
                xaxis_title="Date",
                yaxis_title=y_label,
                showlegend=True,
                annotations=[{
                    'text': 'No data available',
                    'xref': 'paper',
                    'yref': 'paper',
                    'showarrow': False,
                    'font': {'size': 16}
                }]
            )
            return fig
        
        # Determine aggregation label for display
        date_range_days = (df_trends['date'].max() - df_trends['date'].min()).days if not df_trends.empty else 0
        if date_range_days > 180:
            x_label = 'Month'
        elif date_range_days > 60:
            x_label = 'Week'
        else:
            x_label = 'Date'
        
        # Create line chart with connected lines
        fig = px.line(
            df_trends,
            x='date',
            y='value',
            color='area',
            markers=True,
            title='',
            labels={'date': x_label, 'value': y_label, 'area': 'Neighborhood'}
        )
        
        # Set hover text based on metric
        hover_text = "hazard score" if metric == "hazard" else "crimes"
        
        fig.update_layout(
            height=350,
            xaxis_title=x_label,
            yaxis_title=y_label,
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            ),
            hovermode='x unified',
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            hoverlabel=dict(
                bgcolor="rgba(30, 30, 30, 0.95)",
                font_size=13,
                font_color="white",
                bordercolor="rgba(255, 255, 255, 0.3)"
            )
        )
        
        fig.update_traces(
            mode='lines+markers',
            line=dict(width=2, shape='spline', smoothing=1.0),  # Smooth curved lines
            marker=dict(size=5),
            connectgaps=False,  # Don't connect across missing data
            hovertemplate=f'%{{y:.1f}} {hover_text}<extra></extra>'
        )
        
        return fig
        
    except Exception as e:
        print(f"Error creating trend chart: {e}")
        import traceback
        traceback.print_exc()
        
        fig = px.line()
        fig.update_layout(
            height=350,
            annotations=[{
                'text': f'Error: {str(e)}',
                'xref': 'paper',
                'yref': 'paper',
                'showarrow': False,
                'font': {'size': 14}
            }]
        )
        return fig

# Handle drill-down level changes
@app.callback(
    Output("drill-level-store", "data"),
    Output("drill-back-btn", "style"),
    Output("bar-chart-subtitle", "children"),
    Output("selected-category-store", "data"),
    Output("click-reset-counter", "data"),
    Input("bar-crime-types", "clickData"),
    Input("drill-back-btn", "n_clicks"),
    State("drill-level-store", "data"),
    State("selected-category-store", "data"),
    State("click-reset-counter", "data"),
    prevent_initial_call=False
)
def update_drill_level(click_data, back_clicks, current_level, current_category, reset_counter):
    """Handle drill-down between category and subcategory views."""
    from dash import callback_context, no_update
    
    reset_counter = reset_counter or 0
    
    if not callback_context.triggered:
        return "category", {"display": "none"}, "Click on a category to drill down", None, reset_counter
    
    trigger_id = callback_context.triggered[0]['prop_id'].split('.')[0]
    
    print(f"DEBUG DRILL: trigger_id={trigger_id}, current_level={current_level}, current_category={current_category}, reset_counter={reset_counter}")
    print(f"DEBUG DRILL: click_data={click_data}")
    
    # Back button clicked - go back to category level and increment reset counter
    if trigger_id == "drill-back-btn":
        print(f"DEBUG DRILL: Back button clicked, resetting to category view and incrementing counter")
        return "category", {"display": "none"}, "Click on a category to drill down", None, reset_counter + 1
    
    # Bar chart clicked
    if trigger_id == "bar-crime-types":
        if not click_data or 'points' not in click_data:
            print(f"DEBUG DRILL: No valid click data, maintaining current state")
            return no_update, no_update, no_update, no_update, no_update
            
        if current_level == "category":
            # Get the clicked category from the x-axis value (main category)
            clicked_point = click_data['points'][0]
            clicked_category = clicked_point.get('x')  # This is the main category
            
            print(f"DEBUG DRILL: Drilling down into category: {clicked_category}")
            if clicked_category:
                return "subcategory", {"display": "inline-block"}, f"Showing {clicked_category} sub-categories • Click ← Back to return to overview", clicked_category, reset_counter
            else:
                return no_update, no_update, no_update, no_update, no_update
        elif current_level == "subcategory":
            # Already at subcategory level - don't drill further
            print(f"DEBUG DRILL: Already at subcategory level, ignoring click")
            return no_update, no_update, no_update, no_update, no_update
    
    # Fallback - no change
    print(f"DEBUG DRILL: Fallback - no updates")
    return no_update, no_update, no_update, no_update, no_update

# Update crime type bar chart - stacked bars with subcategories
@app.callback(
    Output("bar-crime-types", "figure"),
    Input("drill-level-store", "data"),
    Input("selected-category-store", "data"),
    Input("click-reset-counter", "data"),
    Input("date-range-picker", "data"),
    prevent_initial_call=False
)
def update_crime_type_chart(drill_level, selected_category, reset_counter, date_data):
    """Update the bar chart with subcategories stacked within each category bar."""
    try:
        start_date = date_data.get("start_date") if date_data else None
        end_date = date_data.get("end_date") if date_data else None
        print(f"DEBUG CHART: drill_level={drill_level}, selected_category={selected_category}")
        
        # Load data and apply date filter
        df = load_all_data()
        if start_date and end_date and not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
        
        if df.empty or 'crime_against_category' not in df.columns or 'offense_sub_category' not in df.columns:
            fig = px.bar()
            fig.update_layout(
                height=450,
                annotations=[{'text': 'No data available', 'xref': 'paper', 'yref': 'paper', 'showarrow': False}]
            )
            return fig
        
        if drill_level == "category":
            # Category overview - stacked bar chart with subcategories inside each category
            # Group by category and subcategory
            grouped = df.groupby(['crime_against_category', 'offense_sub_category']).size().reset_index(name='count')
            
            # Calculate total count per category for sorting
            category_totals = grouped.groupby('crime_against_category')['count'].sum().sort_values(ascending=False)
            
            # Sort categories by total count
            sorted_categories = category_totals.index.tolist()
            
            # Define base colors for each category
            category_colors = {
                'PERSON': '#dc3545',      # Red
                'PROPERTY': '#ffc107',    # Yellow/Gold
                'SOCIETY': '#17a2b8',     # Teal
                'ANY': '#9b59b6',         # Purple
                'NOT_A_CRIME': '#6c757d'  # Gray
            }
            
            # Build traces manually for better control over colors and opacity
            import plotly.graph_objects as go
            
            # Helper function to convert hex to RGB
            def hex_to_rgb(hex_color):
                hex_color = hex_color.lstrip('#')
                return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
            
            traces = []
            for category in sorted_categories:
                cat_data = grouped[grouped['crime_against_category'] == category]
                top_subcats = cat_data.nlargest(10, 'count').sort_values('count', ascending=False)
                
                base_color = category_colors.get(category, '#636efa')
                rgb = hex_to_rgb(base_color)
                
                for idx, (_, row) in enumerate(top_subcats.iterrows()):
                    # Vary opacity from 0.9 to 0.4 based on rank
                    opacity = 0.9 - (idx * 0.05)  # Decreases by 0.05 per subcategory
                    opacity = max(opacity, 0.4)  # Minimum opacity of 0.4
                    
                    traces.append(go.Bar(
                        name=row['offense_sub_category'],
                        x=[category],
                        y=[row['count']],
                        marker_color=f'rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, {opacity})',
                        hovertemplate=f'<b>{row["offense_sub_category"]}</b><br>Category: {category}<br>Count: {row["count"]:,}<extra></extra>',
                        showlegend=False
                    ))
            
            fig = go.Figure(data=traces)
            
            fig.update_layout(
                barmode='stack',
                height=350,
                xaxis_title="Crime Category",
                yaxis_title="Total Count",
                xaxis={
                    'categoryorder': 'array', 
                    'categoryarray': sorted_categories,
                    'tickangle': 0,
                    'range': [-0.5, len(sorted_categories) - 0.5]
                },
                showlegend=False,
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                template='plotly_dark',
                margin=dict(l=60, r=30, b=60, t=20),
                bargap=0.4
            )
            
        else:
            # Drill-down mode - show detailed subcategories for selected category
            if selected_category:
                df = df[df['crime_against_category'] == selected_category]
            
            subcats = df['offense_sub_category'].value_counts().head(20)
            
            # Reverse order so highest count is at the top (horizontal bar charts display bottom-to-top by default)
            subcats = subcats[::-1]
            
            # Truncate long labels
            labels = [str(s)[:40] + '...' if len(str(s)) > 40 else str(s) for s in subcats.index]
            
            import plotly.graph_objects as go
            
            category_colors = {
                    'PERSON': '#dc3545',
                    'PROPERTY': '#ffc107',
                'SOCIETY': '#17a2b8',
                'ANY': '#9b59b6',
                'NOT_A_CRIME': '#6c757d'
            }
            
            fig = go.Figure(data=[
                go.Bar(
                    y=labels,
                    x=subcats.values,
                    orientation='h',
                    marker_color=category_colors.get(selected_category, '#636efa'),
                    hovertemplate='<b>%{y}</b><br>Count: %{x:,}<extra></extra>'
                )
            ])
        
        fig.update_layout(
                height=350,
                xaxis_title="Count",
                yaxis_title="Offense Type",
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=200, r=50, t=30, b=50),
                yaxis={'automargin': True}
        )
        
        return fig
        
    except Exception as e:
        print(f"Error creating crime type chart: {e}")
        import traceback
        traceback.print_exc()
        
        fig = px.bar()
        fig.update_layout(
            height=350,
            annotations=[{
                'text': f'Error: {str(e)}',
                'xref': 'paper',
                'yref': 'paper',
                'showarrow': False,
                'font': {'size': 14}
            }]
        )
        return fig

# Separate callback for KPIs - only updates on date changes
@app.callback(
    Output("kpi-total", "children"),
    Output("kpi-property", "children"),
    Output("kpi-person", "children"),
    Output("kpi-society", "children"),
    Input("date-range-picker", "data"),
    prevent_initial_call=False
)
def update_kpis(date_data):
    """Update KPIs based only on date range filter."""
    try:
        start_date = date_data.get("start_date") if date_data else None
        end_date = date_data.get("end_date") if date_data else None
        # For KPIs: use date-filtered data (but not filtered by other criteria)
        df_kpi = load_all_data()
        if start_date and end_date and not df_kpi.empty:
            df_kpi['date'] = pd.to_datetime(df_kpi['date'])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df_kpi = df_kpi[(df_kpi['date'] >= start) & (df_kpi['date'] <= end)].copy()
            print(f"DEBUG KPI: Applied date filter to KPIs - {len(df_kpi)} records")
        
        total = len(df_kpi)
        
        # Calculate KPIs based on date-filtered data
        person = int((df_kpi["crime_against_category"] == "PERSON").sum()) if total and "crime_against_category" in df_kpi.columns else 0
        property_ = int((df_kpi["crime_against_category"] == "PROPERTY").sum()) if total and "crime_against_category" in df_kpi.columns else 0
        society = int((df_kpi["crime_against_category"] == "SOCIETY").sum()) if total and "crime_against_category" in df_kpi.columns else 0
        
        # Debug: Check what categories exist and their counts
        if "crime_against_category" in df_kpi.columns:
            category_counts = df_kpi["crime_against_category"].value_counts()
            print(f"DEBUG KPI: Category breakdown:")
            print(category_counts)
            print(f"DEBUG KPI: Total from categories: {person + property_ + society} vs Total records: {total}")
        
        # Calculate percentages
        person_pct = (person / total * 100) if total > 0 else 0
        property_pct = (property_ / total * 100) if total > 0 else 0
        society_pct = (society / total * 100) if total > 0 else 0
        
        # Format displays with count and percentage
        property_display = html.Div([
            html.Div(f"{property_:,}", style={"fontSize": "2.5rem", "fontWeight": "bold"}),
            html.Div(f"({property_pct:.1f}%)", style={"fontSize": "0.9rem", "color": "#888", "marginTop": "-5px"})
        ])
        
        person_display = html.Div([
            html.Div(f"{person:,}", style={"fontSize": "2.5rem", "fontWeight": "bold"}),
            html.Div(f"({person_pct:.1f}%)", style={"fontSize": "0.9rem", "color": "#888", "marginTop": "-5px"})
        ])
        
        society_display = html.Div([
            html.Div(f"{society:,}", style={"fontSize": "2.5rem", "fontWeight": "bold"}),
            html.Div(f"({society_pct:.1f}%)", style={"fontSize": "0.9rem", "color": "#888", "marginTop": "-5px"})
        ])
        
        total_display = html.Div([
            html.Div(f"{total:,}", style={"fontSize": "2.5rem", "fontWeight": "bold"}),
            html.Div("(100%)", style={"fontSize": "0.9rem", "color": "#888", "marginTop": "-5px"})
        ])
        
        return (
            total_display,
            property_display,
            person_display,
            society_display
        )
        
    except Exception as e:
        print(f"Error calculating KPIs: {e}")
        import traceback
        traceback.print_exc()
        zero_display = html.Div([
            html.Div("0", style={"fontSize": "2.5rem", "fontWeight": "bold"}),
            html.Div("(0%)", style={"fontSize": "0.9rem", "color": "#888", "marginTop": "-5px"})
        ])
        return zero_display, zero_display, zero_display, zero_display

# Separate callback for map and table - updates on all filters
@app.callback(
    Output("details-table", "data"),
    Input("poly-store", "data"),
    Input("hour-slider", "data"),
    Input("category-filter", "value"),
    Input("neighborhood-filter", "value"),
    Input("address-coords-store", "data"),
    Input("circle-radius-input", "value"),
    Input("date-range-picker", "data"),
    prevent_initial_call=False
)
def update_table(poly_geojson, hour_value, category_value, neighborhood_value, circle_coords, circle_radius, date_data):
    start_date = date_data.get("start_date") if date_data else None
    end_date = date_data.get("end_date") if date_data else None
    # Normalize hour selection (range [0,23] == show all)
    hour_range = hour_value if hour_value is not None and isinstance(hour_value, list) else [0, 23]

    # Use the category value directly from dropdown
    selected_categories = category_value if category_value else []
    
    print(f"DEBUG TABLE: Starting update_table callback")
    print(f"DEBUG TABLE: hour_value={hour_value}, hour_range={hour_range}")
    print(f"DEBUG TABLE: category_value={category_value}")
    print(f"DEBUG TABLE: selected_categories={selected_categories}")
    print(f"DEBUG TABLE: poly_geojson present: {poly_geojson is not None}")
    
    # If no crime types selected, return empty table
    if not selected_categories or len(selected_categories) == 0:
        print(f"DEBUG TABLE: No crime types selected - returning empty table")
        return []

    # 1) Load data from cache (only queries warehouse once at startup!)
    try:
        # Use cached data - this prevents querying the warehouse on every interaction
        df = load_all_data()
        print(f"DEBUG TABLE: Loaded {len(df)} rows from cache")
        print(f"DEBUG TABLE: Columns: {df.columns.tolist()}")
        
        # Apply date range filter FIRST (before any other filters)
        if start_date and end_date:
            df['date'] = pd.to_datetime(df['date'])
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df['date'] >= start) & (df['date'] <= end)].copy()
            print(f"DEBUG TABLE: After date filter ({start_date} to {end_date}): {len(df)} rows")
        
        # Filter out records without coordinates
        df = df[df['latitude'].notna() & df['longitude'].notna()].copy()
        print(f"DEBUG TABLE: After lat/lon filter: {len(df)} rows")
        
        # Apply polygon filter if drawn
        if poly_geojson and not df.empty:
            min_lat, max_lat, min_lon, max_lon = geojson_to_polygon_filter(poly_geojson)
            df = df[
                (df['latitude'] >= min_lat) & (df['latitude'] <= max_lat) &
                (df['longitude'] >= min_lon) & (df['longitude'] <= max_lon)
            ].copy()
            
            # Filter by actual polygon
            polygon_coords = poly_geojson["geometry"]["coordinates"][0]
            mask = df.apply(lambda row: point_in_polygon(row['latitude'], row['longitude'], polygon_coords), axis=1)
            df = df[mask]
            print(f"DEBUG TABLE: After polygon filter: {len(df)} rows")
            
    except Exception as e:
        print(f"Error loading data: {e}")
        import traceback
        traceback.print_exc()
        # Safe fallback - return empty dataframe with required columns
        df = pd.DataFrame(columns=["date", "time", "hour", "offense", "offense_sub_category", "crime_against_category", "location", "area", "precinct", "sector", "hazardness", "latitude", "longitude", "datetime"])

    # Apply hour range filter (24 means midnight end, treat as 23 for filtering)
    if hour_range and len(hour_range) == 2:
        start_hour, end_hour = hour_range
        end_hour_filter = min(end_hour, 23)  # Cap at 23 since data uses 0-23
        if not (start_hour == 0 and end_hour == 24):  # Not all hours
            df = df[(df["hour"] >= start_hour) & (df["hour"] <= end_hour_filter)]
            print(f"DEBUG TABLE: After hour filter ({start_hour}-{end_hour_filter}): {len(df)} rows")

    # Apply crime type (category) filter
    if selected_categories and len(selected_categories) > 0:
        if "crime_against_category" in df.columns:
            df = df[df["crime_against_category"].isin(selected_categories)]
            print(f"DEBUG TABLE: After crime type filter ({selected_categories}): {len(df)} rows")

    # Apply neighborhood filter
    if neighborhood_value:
        if isinstance(neighborhood_value, str):
            neighborhoods = [neighborhood_value]
        else:
            neighborhoods = neighborhood_value
        if "area" in df.columns and neighborhoods:
            df = df[df["area"].isin(neighborhoods)]
            print(f"DEBUG TABLE: After neighborhood filter ({neighborhoods}): {len(df)} rows")

    # Apply circle filter (always active when circle exists)
    if circle_coords:
        print(f"DEBUG: Applying circle filter - center: ({circle_coords['lat']}, {circle_coords['lon']}), radius: {circle_radius}m")
        if not df.empty and 'latitude' in df.columns and 'longitude' in df.columns:
            # Calculate distance for each point
            center_lat = circle_coords['lat']
            center_lon = circle_coords['lon']
            radius = circle_radius if circle_radius else 805  # Default: 0.5 miles
            
            # Filter points within the circle
            df = df[df.apply(
                lambda row: haversine_distance(center_lat, center_lon, row['latitude'], row['longitude']) <= radius
                if pd.notna(row['latitude']) and pd.notna(row['longitude']) else False,
                axis=1
            )].copy()
            print(f"DEBUG: After circle filter: {len(df)} points")

    # Store filtered data for table
    df_filtered = df.copy()
    print(f"DEBUG TABLE: df_filtered has {len(df_filtered)} rows")

    # Create table (top 500) - use df_filtered to match what's shown
    # Select meaningful columns for display - use datetime column for table
    table_columns = [col for col in ["datetime", "offense_sub_category", "crime_against_category", "location", "area", "hazardness"] if col in df_filtered.columns]
    
    if not df_filtered.empty and table_columns:
        # Sort by datetime (most recent first) before taking top 500
        df_sorted = df_filtered.copy()
        if 'datetime' in df_sorted.columns:
            df_sorted['datetime'] = pd.to_datetime(df_sorted['datetime'], errors='coerce')
            df_sorted = df_sorted.sort_values('datetime', ascending=False)
        elif 'date' in df_sorted.columns:
            # Fallback to date if datetime not available
            df_sorted['date'] = pd.to_datetime(df_sorted['date'], errors='coerce')
            df_sorted = df_sorted.sort_values('date', ascending=False)
        
        # Format the data for display
        table_df = df_sorted[table_columns].head(500).copy()
        
        # Format datetime column for display
        if 'datetime' in table_df.columns:
            table_df['datetime'] = pd.to_datetime(table_df['datetime'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Round hazardness if present
        if 'hazardness' in table_df.columns:
            table_df['hazardness'] = table_df['hazardness'].round(1)
        
        # Rename columns for better display
        rename_map = {
            'datetime': 'Date & Time',
            'offense_sub_category': 'Offense Type',
            'crime_against_category': 'Crime Against',
            'hazardness': 'Hazard Score'
        }
        table_df = table_df.rename(columns={k: v for k, v in rename_map.items() if k in table_df.columns})
        
        table = table_df.to_dict("records")
    else:
        table = []

    print(f"DEBUG TABLE: Returning {len(table)} table records")
    
    return table


# Plotly Map Update Callback
@app.callback(
    Output("crime-map", "figure"),
    Output("map-stats", "children"),
    Input("poly-store", "data"),
    Input("hour-slider", "data"),
    Input("category-filter", "value"),
    Input("neighborhood-filter", "value"),
    Input("address-coords-store", "data"),
    Input("circle-radius-input", "value"),
    Input("date-range-picker", "data"),
    prevent_initial_call=False
)
def update_map_points(poly_geojson, hour_value, category_value, neighborhood_value, circle_coords, circle_radius, date_data):
    """Update Plotly scattermapbox with crime points."""
    from dash import callback_context
    start_date = date_data.get("start_date") if date_data else None
    end_date = date_data.get("end_date") if date_data else None
    import plotly.graph_objects as go
    
    # Check if address search triggered this callback
    triggered = callback_context.triggered[0]['prop_id'] if callback_context.triggered else ""
    center_on_circle = "address-coords-store" in triggered
    
    # Empty map template
    empty_fig = go.Figure(go.Scattermapbox())
    empty_fig.update_layout(
        mapbox=dict(style="carto-darkmatter", center=dict(lat=47.6062, lon=-122.3321), zoom=10),
        margin=dict(l=0, r=0, t=0, b=0)
    )
    
    try:
        if not category_value or len(category_value) == 0:
            return empty_fig, "Select at least one crime type"
        
        df = load_all_data()
        if df is None or df.empty:
            return empty_fig, "No data"
        
        df = df.copy()
        
        # Date filter - ensure proper datetime comparison
        if start_date and end_date:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
        
        # Hour range filter (24 means midnight end, treat as 23 for filtering)
        if hour_value is not None and isinstance(hour_value, list) and len(hour_value) == 2:
            start_hour, end_hour = hour_value
            end_hour_filter = min(end_hour, 23)  # Cap at 23 since data uses 0-23
            if not (start_hour == 0 and end_hour == 24):  # Not all hours
                df = df[(df['hour'] >= start_hour) & (df['hour'] <= end_hour_filter)]
        
        # Category filter
        df = df[df['crime_against_category'].isin(category_value)]
        
        # Neighborhood filter
        if neighborhood_value and len(neighborhood_value) > 0:
            df = df[df['area'].isin(neighborhood_value if isinstance(neighborhood_value, list) else [neighborhood_value])]
        
        # Circle filter
        if circle_coords:
            radius = circle_radius if circle_radius else 805
            df = df[df.apply(lambda r: haversine_distance(circle_coords['lat'], circle_coords['lon'], r['latitude'], r['longitude']) <= radius if pd.notna(r['latitude']) else False, axis=1)]
        
        # Valid coordinates
        df = df[df['latitude'].notna() & df['longitude'].notna()]
        
        if df.empty:
            return empty_fig, "No data matches filters"
        
        # Limit for performance
        total = len(df)
        df = df.sort_values('date', ascending=False).head(5000)
        
        # Calculate frequency at each location (rounded to 4 decimal places for grouping nearby points)
        df['loc_key'] = df['latitude'].round(4).astype(str) + ',' + df['longitude'].round(4).astype(str)
        location_counts = df.groupby('loc_key').size().to_dict()
        df['location_freq'] = df['loc_key'].map(location_counts)
        
        # Scale size: min 5, max 25, based on frequency
        min_freq, max_freq = df['location_freq'].min(), df['location_freq'].max()
        if max_freq > min_freq:
            df['marker_size'] = 5 + (df['location_freq'] - min_freq) / (max_freq - min_freq) * 20
        else:
            df['marker_size'] = 8
        
        # Color map - matches bar chart colors
        colors = {'PERSON': '#dc3545', 'PROPERTY': '#ffc107', 'SOCIETY': '#17a2b8'}
        
        fig = go.Figure()
        
        for cat in df['crime_against_category'].unique():
            cat_df = df[df['crime_against_category'] == cat]
            fig.add_trace(go.Scattermapbox(
                lat=cat_df['latitude'],
                lon=cat_df['longitude'],
                mode='markers',
                marker=dict(
                    size=cat_df['marker_size'],
                    color=colors.get(cat, 'gray'),
                    opacity=0.7,
                    sizemode='diameter'
                ),
                name=cat,
                hovertemplate=f"{cat}<br>%{{text}}<br>Incidents at location: %{{customdata}}<extra></extra>",
                text=cat_df['offense_sub_category'].fillna('Unknown'),
                customdata=cat_df['location_freq']
            ))
        
        # Draw circle if address search is active
        if circle_coords:
            import numpy as np
            center_lat = circle_coords['lat']
            center_lon = circle_coords['lon']
            radius_m = circle_radius if circle_radius else 805
            
            # Generate circle points (approximation using bearing calculation)
            num_points = 64
            circle_lats = []
            circle_lons = []
            for i in range(num_points + 1):
                angle = 2 * np.pi * i / num_points
                # Approximate: 1 degree lat ≈ 111km, 1 degree lon ≈ 111km * cos(lat)
                dlat = (radius_m / 111000) * np.cos(angle)
                dlon = (radius_m / (111000 * np.cos(np.radians(center_lat)))) * np.sin(angle)
                circle_lats.append(center_lat + dlat)
                circle_lons.append(center_lon + dlon)
            
            # Add circle outline
            fig.add_trace(go.Scattermapbox(
                lat=circle_lats,
                lon=circle_lons,
                mode='lines',
                line=dict(width=3, color='#FFA500'),
                fill='toself',
                fillcolor='rgba(255, 165, 0, 0.15)',
                name='Search Area',
                hoverinfo='skip'
            ))
            
            # Add center marker
            fig.add_trace(go.Scattermapbox(
                lat=[center_lat],
                lon=[center_lon],
                mode='markers',
                marker=dict(size=12, color='#FFA500', symbol='circle'),
                name='Search Center',
                hovertemplate=f"Search Center<br>{circle_coords.get('display_name', '')[:40]}...<extra></extra>"
            ))
        
        # Determine map center and zoom
        if circle_coords and center_on_circle:
            # Center on circle when address is searched
            map_center = dict(lat=circle_coords['lat'], lon=circle_coords['lon'])
            # Adjust zoom based on radius (smaller radius = higher zoom)
            radius_m = circle_radius if circle_radius else 805
            if radius_m <= 400:
                map_zoom = 15
            elif radius_m <= 800:
                map_zoom = 14
            elif radius_m <= 1600:
                map_zoom = 13
            else:
                map_zoom = 12
            # Change uirevision to force re-center
            ui_revision = f"circle_{circle_coords['lat']}_{circle_coords['lon']}"
        else:
            # Default Seattle center
            map_center = dict(lat=47.6062, lon=-122.3321)
            map_zoom = 11
            ui_revision = "constant"  # Preserves zoom/pan state
        
        fig.update_layout(
            mapbox=dict(style="carto-darkmatter", center=map_center, zoom=map_zoom),
            margin=dict(l=0, r=0, t=0, b=0),
            showlegend=False,  # Legend removed - colors shown in crime type filter
            uirevision=ui_revision
        )
        
        counts = df['crime_against_category'].value_counts().to_dict()
        stats = f"{len(df):,} points | {counts}" + (f" (of {total:,})" if total > 5000 else "")
        
        return fig, stats
        
    except Exception as e:
        print(f"Map error: {e}")
        return empty_fig, f"Error: {e}"


if __name__ == "__main__":
    # Check which data file exists
    if os.path.exists(PARQUET_FILE_PATH):
        print(f"📁 Data source: {PARQUET_FILE_PATH} (Parquet - optimized)")
    elif os.path.exists(CSV_FILE_PATH):
        print(f"📁 Data source: {CSV_FILE_PATH} (CSV)")
        print("💡 Tip: Convert to Parquet for better performance: python convert_to_parquet.py")
    else:
        print(f"⚠️  Warning: No data file found")
        print(f"   Expected: {PARQUET_FILE_PATH} or {CSV_FILE_PATH}")
    print()

    # Pre-warm the data cache on startup
    print("🔥 Loading data...")
    try:
        initial_data = load_all_data()
        if initial_data is not None and len(initial_data) > 0:
            print(f"✅ Data loaded successfully with {len(initial_data):,} records")
        else:
            print("⚠️  WARNING: No data loaded! Check the data file.")
    except Exception as e:
        print(f"❌ ERROR: Failed to load data: {str(e)}")
        print("⚠️  The app will start but visualizations may not work.")
    
    port = int(os.environ.get('PORT', 8050))
    print(f"\n{'='*60}")
    print(f"🚀 Server running at http://localhost:{port}")
    print(f"{'='*60}\n")
    print("Press Ctrl+C to stop the server.\n")
    
    # Disable automatic browser opening
    debug_mode = os.environ.get("DASH_DEBUG", "1") == "1"

    app.run(host='0.0.0.0', port=port, debug=debug_mode, dev_tools_hot_reload=True)
