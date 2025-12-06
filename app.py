#!/usr/bin/env python3

"""
Seattle Crime Data Dashboard (Dash)
- Reads from SQLite database (crime_data_gold.db) downloaded from Google Drive
- Provides: date picker, KPI cards, offense distribution pie chart,
            time-of-day bar chart, map, and details table.
- Memory efficient: queries only needed data, doesn't load all data into memory

Run:
  pip install -r requirements.txt
  python app.py
Then open http://127.0.0.1:8050
"""

import os
import re
import time
import requests
from datetime import datetime

import pandas as pd
import numpy as np
import plotly.express as px
import dash
from dash import Dash, html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc
from dash import dash_table
import dash_mantine_components as dmc
import sqlite3
import gdown

# Data file path (relative to app.py)
DB_FILE_PATH = os.path.join(os.path.dirname(__file__), 'crime_data_gold.db')

# Google Drive URL for SQLite database (from environment variable or default)
DB_GDRIVE_URL = os.environ.get('DB_GDRIVE_URL', 'https://drive.google.com/file/d/1ktC0b1KUwIYJLnXHjD0T0PpSLVo6npL6/view?usp=drive_link')

def get_google_drive_file_id(url: str) -> str:
    """Extract file ID from Google Drive URL."""
    if "drive.google.com" in url:
        if "/file/d/" in url:
            return url.split("/file/d/")[1].split("/")[0]
        elif "id=" in url:
            return url.split("id=")[1].split("&")[0]
    return None

def download_from_google_drive(file_id: str, destination: str):
    """Download a file from Google Drive, handling large file confirmation."""
    session = requests.Session()
    
    # First request to get cookies and potential confirmation token
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = session.get(url, stream=True)
    response.raise_for_status()
    
    # Check for confirmation token in response
    token = None
    
    # Method 1: Check cookies for download_warning token
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            token = value
            break
    
    # Method 2: Try to extract token from HTML response
    if token is None:
        content_type = response.headers.get('Content-Type', '')
        if 'text/html' in content_type:
            content = response.content.decode('utf-8', errors='ignore')
            match = re.search(r'confirm=([0-9A-Za-z_-]+)', content)
            if match:
                token = match.group(1)
    
    # If we found a token, make a new request with confirmation
    if token:
        url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm={token}"
        response = session.get(url, stream=True)
        response.raise_for_status()
    
    # Final check: if still HTML, try direct download URL with confirm=t
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type:
        url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
        response = session.get(url, stream=True)
        response.raise_for_status()
    
    # Write the file
    with open(destination, "wb") as f:
        for chunk in response.iter_content(1024 * 1024):  # 1MB chunks
            if chunk:
                f.write(chunk)
    
    # Verify the downloaded file is not HTML
    with open(destination, "rb") as f:
        header = f.read(100)
        if b'<!DOCTYPE' in header or b'<html' in header.lower():
            os.remove(destination)
            raise Exception("Downloaded file is HTML, not the actual database file. Please check Google Drive sharing settings.")

def ensure_db_file_exists():
    """Download SQLite database from Google Drive if it doesn't exist locally."""
    if os.path.exists(DB_FILE_PATH):
        print(f"✅ SQLite database exists: {DB_FILE_PATH}")
        return True
    
    # Try to download from Google Drive if URL is provided
    if DB_GDRIVE_URL:
        print(f"📥 Downloading SQLite database from Google Drive...")
        print(f"   URL: {DB_GDRIVE_URL}")
        try:
            # Check if it's a Google Drive URL
            file_id = get_google_drive_file_id(DB_GDRIVE_URL)
            
            if file_id:
                print(f"   Detected Google Drive file ID: {file_id}")
                download_from_google_drive(file_id, DB_FILE_PATH)
            else:
                # Try gdown as fallback
                gdown.download(DB_GDRIVE_URL, DB_FILE_PATH, quiet=False, fuzzy=True)
            
            if os.path.exists(DB_FILE_PATH):
                print(f"✅ Downloaded SQLite database to: {DB_FILE_PATH}")
        return True
            else:
                print(f"❌ Download completed but file not found at: {DB_FILE_PATH}")
                return False
    except Exception as e:
            print(f"❌ Failed to download SQLite database: {e}")
            import traceback
            traceback.print_exc()
        return False

    print("⚠️  No SQLite database found locally and no Google Drive URL provided")
    return False

# Download database file on module load (for gunicorn) - only if file doesn't exist
if not os.path.exists(DB_FILE_PATH):
    ensure_db_file_exists()

# Database connection (thread-safe for SQLite)
_db_connection = None

def get_db_connection():
    """Get SQLite database connection (thread-safe)."""
    global _db_connection
    if _db_connection is None and os.path.exists(DB_FILE_PATH):
        _db_connection = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
    return _db_connection

def get_date_range():
    """Get min and max date from database (query directly, don't load all data)."""
    try:
        conn = get_db_connection()
        if not conn:
            return {'min': '', 'max': ''}
        
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(date), MAX(date) FROM crimes WHERE date IS NOT NULL;")
        result = cursor.fetchone()
        if result and result[0] and result[1]:
            return {'min': str(result[0]), 'max': str(result[1])}
    except Exception as e:
        print(f"Error getting date range: {e}")
    return {'min': '', 'max': ''}

def query_data_by_date_range(start_date=None, end_date=None, limit=None):
    """
    Query data from SQLite by date range (memory efficient - only loads what's needed).
    
    Args:
        start_date: Start date (YYYY-MM-DD format) or None
        end_date: End date (YYYY-MM-DD format) or None
        limit: Maximum number of rows to return (None = no limit)
        
    Returns:
        DataFrame with crime data
    """
    try:
        conn = get_db_connection()
        if not conn:
            print(f"❌ Failed to get database connection")
            return pd.DataFrame()
        
        # Build query with date filters
        query = """
        SELECT 
            date, time, hour, datetime, offense, offense_sub_category,
            crime_against_category, location, area, precinct, sector,
            hazardness, latitude, longitude
        FROM crimes
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """
        
        params = []
        
        # Add date filters
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        # Add limit if specified
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute query
        df = pd.read_sql_query(query, conn, params=params if params else None)
        
        # Convert date column to datetime
        if 'date' in df.columns and not df.empty:
            df['date'] = pd.to_datetime(df['date'])
        
        # Optimize memory usage
        if not df.empty:
            # Convert string columns to category
            string_cols = ['offense', 'offense_sub_category', 'crime_against_category', 
                          'area', 'precinct', 'sector', 'time']
            for col in string_cols:
                if col in df.columns:
                    df[col] = df[col].astype('category')
            
            # Optimize numeric types
            if 'hour' in df.columns:
                df['hour'] = df['hour'].astype('int8')
            if 'hazardness' in df.columns:
                df['hazardness'] = pd.to_numeric(df['hazardness'], errors='coerce').astype('float32')
            if 'latitude' in df.columns:
                df['latitude'] = df['latitude'].astype('float32')
            if 'longitude' in df.columns:
                df['longitude'] = df['longitude'].astype('float32')
        
        return df
        
    except Exception as e:
        print(f"❌ Error querying data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def load_all_data(force_refresh=False, start_date=None, end_date=None):
    """
    Load data from SQLite database with date range filter (memory efficient).
    
    Args:
        force_refresh: Ignored (kept for compatibility)
        start_date: Start date (YYYY-MM-DD format) or None
        end_date: End date (YYYY-MM-DD format) or None
        
    Returns:
        DataFrame with crime data
    """
    # Convert datetime objects to strings if needed
    if start_date and isinstance(start_date, (datetime, pd.Timestamp)):
        start_date = start_date.strftime('%Y-%m-%d')
    if end_date and isinstance(end_date, (datetime, pd.Timestamp)):
        end_date = end_date.strftime('%Y-%m-%d')
    
    return query_data_by_date_range(start_date=start_date, end_date=end_date)

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
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r


# ---------------------------- App ----------------------------

app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
app.title = "Seattle Crime Dashboard"
server = app.server  # Expose for gunicorn

# Get initial date range from DB (lazy load - will be loaded on first request)
# Don't load data at module import time to avoid blocking gunicorn startup
initial_date_range = get_date_range()

# Continue with the rest of your app.py layout and callbacks...
# (I'll need to see the rest of the file to complete this)
