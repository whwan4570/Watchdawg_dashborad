#!/usr/bin/env python3
"""
Convert crime_data_gold.csv to SQLite database for better memory efficiency.

Benefits of SQLite:
- Only load data when needed (query-based)
- Much lower memory usage (no need to load all 563K records at once)
- Fast queries with indexes
- Can filter data before loading into memory

Usage:
    python convert_to_sqlite.py

This will create crime_data_gold.db in the same directory.
"""

import os
import pandas as pd
import sqlite3

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(SCRIPT_DIR, 'crime_data_gold.csv')
DB_FILE = os.path.join(SCRIPT_DIR, 'crime_data_gold.db')

def convert_csv_to_sqlite():
    """Convert CSV file to SQLite database."""
    
    # Check if CSV exists
    if not os.path.exists(CSV_FILE):
        print(f"❌ Error: {CSV_FILE} not found!")
        print(f"   Please ensure crime_data_gold.csv is in the same directory as this script.")
        print(f"   Script location: {SCRIPT_DIR}")
        return False
    
    print(f"📖 Reading {CSV_FILE}...")
    try:
        # Read CSV in chunks to save memory
        csv_size = os.path.getsize(CSV_FILE) / (1024 * 1024)  # Size in MB
        print(f"📊 CSV file size: {csv_size:.2f} MB")
        
        # Remove old database if exists
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
            print(f"🗑️  Removed old database file")
        
        # Create SQLite connection
        conn = sqlite3.connect(DB_FILE)
        print(f"💾 Creating SQLite database: {DB_FILE}")
        
        # Read CSV in chunks and write to database
        chunk_size = 50000  # Process 50k rows at a time
        total_rows = 0
        
        print(f"📥 Processing CSV in chunks of {chunk_size:,} rows...")
        for chunk_num, chunk_df in enumerate(pd.read_csv(CSV_FILE, chunksize=chunk_size), 1):
            # Filter out records without coordinates
            chunk_df = chunk_df[chunk_df['Latitude'].notna() & chunk_df['Longitude'].notna()].copy()
            
            # Create derived columns
            chunk_df['date'] = pd.to_datetime(
                chunk_df['Offense Year'].astype(int).astype(str) + '-' + 
                chunk_df['Offense Month'].astype(int).astype(str).str.zfill(2) + '-' + 
                chunk_df['Offense Day'].astype(int).astype(str).str.zfill(2),
                format='%Y-%m-%d',
                errors='coerce'
            )
            
            chunk_df['time'] = chunk_df['Offense Time'].astype(str)
            chunk_df['hour'] = pd.to_datetime(chunk_df['Offense Time'], format='%H:%M:%S', errors='coerce').dt.hour
            chunk_df['hour'] = chunk_df['hour'].fillna(0).astype(int)
            chunk_df['datetime'] = chunk_df['date'].astype(str) + ' ' + chunk_df['Offense Time'].astype(str)
            
            # Rename columns
            chunk_df = chunk_df.rename(columns={
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
            result_df = chunk_df[['date', 'time', 'hour', 'datetime', 'offense', 'offense_sub_category',
                                 'crime_against_category', 'location', 'area', 'precinct', 'sector',
                                 'hazardness', 'latitude', 'longitude']].copy()
            
            # Write to database (append mode after first chunk)
            if chunk_num == 1:
                result_df.to_sql('crimes', conn, if_exists='replace', index=False)
            else:
                result_df.to_sql('crimes', conn, if_exists='append', index=False)
            
            total_rows += len(result_df)
            print(f"   Processed chunk {chunk_num}: {len(result_df):,} rows (total: {total_rows:,})")
        
        # Create indexes for faster queries
        print(f"📊 Creating indexes for faster queries...")
        conn.execute("CREATE INDEX idx_date ON crimes(date)")
        conn.execute("CREATE INDEX idx_hour ON crimes(hour)")
        conn.execute("CREATE INDEX idx_area ON crimes(area)")
        conn.execute("CREATE INDEX idx_category ON crimes(crime_against_category)")
        conn.execute("CREATE INDEX idx_lat_lon ON crimes(latitude, longitude)")
        conn.commit()
        
        # Verify table structure matches app.py expectations
        print(f"🔍 Verifying table structure...")
        cursor = conn.cursor()
        
        # Check table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='crimes';")
        table_check = cursor.fetchone()
        if not table_check:
            print(f"❌ ERROR: Table 'crimes' not found after creation!")
            conn.close()
            return False
        
        # Get table structure
        cursor.execute("PRAGMA table_info(crimes);")
        columns_info = cursor.fetchall()
        actual_columns = [col[1] for col in columns_info]  # col[1] is column name
        
        # Expected columns from app.py
        expected_columns = [
            'date', 'time', 'hour', 'datetime', 'offense', 'offense_sub_category',
            'crime_against_category', 'location', 'area', 'precinct', 'sector',
            'hazardness', 'latitude', 'longitude'
        ]
        
        print(f"   Expected columns: {expected_columns}")
        print(f"   Actual columns: {actual_columns}")
        
        # Check if all expected columns exist
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)
        
        if missing_columns:
            print(f"❌ ERROR: Missing columns: {missing_columns}")
            conn.close()
            return False
        
        if extra_columns:
            print(f"⚠️  WARNING: Extra columns (not used by app): {extra_columns}")
        
        # Verify column order matches (optional but helpful)
        if actual_columns != expected_columns:
            print(f"⚠️  WARNING: Column order differs from expected, but all columns exist")
        
        print(f"✅ Table structure verified successfully!")
        
        # Test query to ensure it works
        print(f"🧪 Testing query...")
        test_query = """
        SELECT 
            date, time, hour, datetime, offense, offense_sub_category,
            crime_against_category, location, area, precinct, sector,
            hazardness, latitude, longitude
        FROM crimes
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 1
        """
        try:
            test_result = pd.read_sql_query(test_query, conn)
            print(f"✅ Test query successful: {len(test_result)} row(s) returned")
        except Exception as query_error:
            print(f"❌ ERROR: Test query failed: {query_error}")
            conn.close()
            return False
        
        # Get database size
        db_size = os.path.getsize(DB_FILE) / (1024 * 1024)  # Size in MB
        
        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM crimes").fetchone()[0]
        
        # Get count with valid coordinates
        valid_coords_count = conn.execute(
            "SELECT COUNT(*) FROM crimes WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchone()[0]
        
        conn.close()
        
        print(f"✅ Successfully created {DB_FILE}")
        print(f"📊 Database size: {db_size:.2f} MB")
        print(f"📊 Total rows: {row_count:,}")
        print(f"📊 Rows with valid coordinates: {valid_coords_count:,}")
        print(f"💾 Size reduction: {((csv_size - db_size) / csv_size * 100):.1f}% ({csv_size - db_size:.2f} MB saved)")
        print()
        print("🎉 Conversion complete!")
        print(f"   The app will automatically use {DB_FILE} if it exists.")
        print()
        print("📤 Next steps:")
        print("   1. Commit the database file to GitHub")
        print("   2. The app will automatically use it instead of CSV/Parquet")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    convert_csv_to_sqlite()

