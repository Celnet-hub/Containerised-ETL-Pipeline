### ETL Pipeline Script ######
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
import os
from sqlalchemy import create_engine

log_file = "log_file.txt"
final_result = "transformed_internet_users_data.csv"
db_name = "customers.db"
table_name = "internet_users"

# Get columns
columns = pd.read_csv('./internet_users.csv')
print(columns.columns)

###### Extraction ########


def extract_from_csv(csv_file):
    dataframe = pd.read_csv(csv_file)
    return dataframe


def extract():
    # create an empty dataframe to hold extracted data
    extracted_data = pd.DataFrame(columns=['Location', 'Rate (WB)', 'Year', 'Rate (ITU)', 'Year.1', 'Users (CIA)',
                                           'Year.2', 'Notes'])

    # begin processing files
    for csvfile in glob.glob("*.csv"):
        if csvfile != final_result:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(
                extract_from_csv(csvfile))], ignore_index=True)

    return extracted_data


############### Transformation ######################

def transform(data):
    # data.columns = data.columns.str.lower()

    # Convert year columns to integers (handle NaN values)
    # Use pandas nullable integer type to properly handle NaN values
    data["Year"] = data["Year"].apply(lambda x: int(
        np.floor(x)) if pd.notna(x) else x).astype('Int64')
    data["Year.1"] = data["Year.1"].apply(lambda x: int(
        np.floor(x)) if pd.notna(x) else x).astype('Int64')
    data["Year.2"] = data["Year.2"].apply(lambda x: int(
        np.floor(x)) if pd.notna(x) else x).astype('Int64')

    data.index.name = "S/N"
    return data


############### Load to CSV and to SQL #################

def connect_to_db():
    # Get database connection parameters from environment variables
    # These are passed by the Docker container via run_etl.sh
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("DB_NAME", "app_db")
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASS = os.getenv("DB_PASSWORD", "admin123")
    DB_PORT = os.getenv("DB_PORT", "5432")

    print(f"Attempting to connect to PostgreSQL at {DB_HOST}:{DB_PORT}")
    print(f"Database: {DB_NAME}, User: {DB_USER}")

    conn = None

    try:
        # --- Establish the connection ---
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        print("Connection successful!")
        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None


def load_data(destination, transformed_data):
    # load to csv
    transformed_data.to_csv(destination)
    print(f"Data saved to CSV: {destination}")

    # Load to PostgreSQL using connection from connect_to_db()
    print("Loading data to PostgreSQL...")

    conn = connect_to_db()
    if conn is None:
        print("Failed to connect to database. Skipping PostgreSQL load.")
        return

    try:
        cur = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            location VARCHAR(255),
            rate_wb FLOAT,
            year_wb INTEGER,
            rate_itu FLOAT,
            year_itu INTEGER,
            users_cia BIGINT,
            year_cia INTEGER,
            notes TEXT
        );
        """
        cur.execute(create_table_query)
        print(f"Table '{table_name}' created or already exists")

        # Clear existing data
        cur.execute(f"DELETE FROM {table_name};")
        print("Cleared existing data from table")

        # Insert data using executemany for better performance
        insert_query = f"""
        INSERT INTO {table_name} (location, rate_wb, year_wb, rate_itu, year_itu, users_cia, year_cia, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Prepare data for bulk insert
        data_to_insert = []
        for index, row in transformed_data.iterrows():
            data_to_insert.append((
                row['Location'],
                row['Rate (WB)'] if pd.notna(row['Rate (WB)']) else None,
                row['Year'] if pd.notna(row['Year']) else None,
                row['Rate (ITU)'] if pd.notna(row['Rate (ITU)']) else None,
                row['Year.1'] if pd.notna(row['Year.1']) else None,
                row['Users (CIA)'] if pd.notna(row['Users (CIA)']) else None,
                row['Year.2'] if pd.notna(row['Year.2']) else None,
                row['Notes'] if pd.notna(row['Notes']) else None
            ))

        # Use executemany for bulk insert
        cur.executemany(insert_query, data_to_insert)

        conn.commit()
        cur.close()
        print(
            f"Successfully loaded {len(data_to_insert)} rows to PostgreSQL table '{table_name}'")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error loading data to PostgreSQL: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed")


def log_progress(msg):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as logs:
        logs.write(timestamp + "," + msg + "\n")


######## Begin ETL ###########
log_progress("Beginning ETL process")

# Test database connection first
print("Testing database connection...")
connect_to_db()

# Extract data
log_progress("Extract phase started")
extracted_data = extract()
log_progress("Extract completed")

# Transform data
log_progress("Beginning transformation")
transformed_data = transform(extracted_data)
log_progress("Transformation completed")

# Load data
log_progress("Beginning Load")
load_data(final_result, transformed_data)
log_progress("Data loaded to CSV and PostgreSQL")

print("ETL process completed successfully!")
