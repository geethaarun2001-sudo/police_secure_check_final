"""
Database Setup and Data Ingestion Script
Connects to MySQL database, creates schema, and ingests cleaned data
"""

import mysql.connector
import pandas as pd
from mysql.connector import Error
import sys
from sql_queries import *

# Database configuration
DB_CONFIG = {
    'host': "gateway01.eu-central-1.prod.aws.tidbcloud.com",
    'port': 4000,
    'user': "4TQcB5BpE3ADNsN.root",
    'password': "NG8gFRIsGag8xXRv",
    'database': "test",
    'ssl_verify_cert': False,
    'ssl_verify_identity': False
}

TABLE_NAME = 'traffic_stops'
CSV_FILE = 'traffic_stops_cleaned.csv'

def create_connection():
    """Create database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print(f"✓ Connected to MySQL database")
            db_info = connection.get_server_info()
            print(f"  Server version: {db_info}")
            return connection
    except Error as e:
        print(f"✗ Error connecting to MySQL: {e}")
        sys.exit(1)

def create_table(connection):
    """Create traffic_stops table with schema"""
    cursor = connection.cursor()
    cursor.execute(get_drop_table_query(TABLE_NAME))
    print(f"✓ Dropped existing table (if any): {TABLE_NAME}")

    cursor.execute(get_create_table_query(TABLE_NAME))
    print(f"✓ Created table: {TABLE_NAME}")
    cursor.close()

def insert_data(connection, df):
    """Insert data from DataFrame into database"""
    cursor = connection.cursor()

    # Convert DataFrame to list of tuples
    data_tuples = [
        (row['stop_date'], row['stop_time'], row['country_name'], row['driver_gender'],
         int(row['driver_age_raw']), int(row['driver_age']), row['driver_race'],
         row['violation_raw'], row['violation'], bool(row['search_conducted']),
         row['search_type'], row['stop_outcome'], bool(row['is_arrested']),
         row['stop_duration'], bool(row['drugs_related_stop']), row['vehicle_number'])
        for _, row in df.iterrows()
    ]

    # Insert in batches
    batch_size = 1000
    total_rows = len(data_tuples)
    print(f"\nInserting {total_rows:,} rows in batches of {batch_size}...")

    for i in range(0, total_rows, batch_size):
        batch = data_tuples[i:i + batch_size]
        cursor.executemany(get_insert_query(TABLE_NAME), batch)
        connection.commit()
        print(f"  Inserted {i+len(batch):,}/{total_rows:,} rows ({(i+len(batch))/total_rows*100:.1f}%)")

    cursor.close()
    print(f"✓ Successfully inserted {total_rows:,} rows")


def verify_data(connection):
    """Verify inserted data"""
    cursor = connection.cursor()

    # Count total rows
    cursor.execute(get_count_query(TABLE_NAME))
    count = cursor.fetchone()[0]
    print(f"\n✓ Verification: {count:,} rows in table")

    # Show sample data
    cursor.execute(get_sample_data_query(TABLE_NAME, 3))
    rows = cursor.fetchall()
    print(f"\nSample data (first 3 rows):")
    for row in rows:
        print(f"  ID: {row[0]}, Date: {row[1]}, Country: {row[3]}, Violation: {row[9]}")

    # Show statistics
    cursor.execute(get_statistics_query(TABLE_NAME))
    stats = cursor.fetchone()
    print(f"\nDatabase Statistics:")
    print(f"  Total stops: {stats[0]:,}")
    print(f"  Countries: {stats[1]}")
    print(f"  Violation types: {stats[2]}")
    print(f"  Arrests: {stats[3]:,}")

    cursor.close()

def main():
    """Main execution function"""
    print("="*60)
    print("DATABASE SETUP AND DATA INGESTION")
    print("="*60)

    # Step 1: Load CSV data
    print(f"\nStep 1: Loading CSV data from {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE)
        print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"✗ Error loading CSV: {e}")
        sys.exit(1)

    # Step 2: Connect to database
    print(f"\nStep 2: Connecting to database")
    connection = create_connection()

    # Step 3: Create table schema
    print(f"\nStep 3: Creating table schema")
    try:
        create_table(connection)
    except Error as e:
        print(f"✗ Error creating table: {e}")
        connection.close()
        sys.exit(1)

    # Step 4: Insert data
    print(f"\nStep 4: Inserting data")
    try:
        insert_data(connection, df)
    except Error as e:
        print(f"✗ Error inserting data: {e}")
        connection.close()
        sys.exit(1)

    # Step 5: Verify data
    print(f"\nStep 5: Verifying data")
    try:
        verify_data(connection)
    except Error as e:
        print(f"✗ Error verifying data: {e}")

    # Close connection
    connection.close()
    print(f"\n✓ Database connection closed")

    print("\n" + "="*60)
    print("DATABASE SETUP COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"\nTable '{TABLE_NAME}' is ready for queries and visualization")

if __name__ == "__main__":
    main()
