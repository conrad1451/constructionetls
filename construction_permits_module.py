# construction_permits_module.py

import os
import time
import logging
import requests
import json
import pandas as pd
from dateutil.parser import parse as parse_date
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from datetime import datetime  # <--- ADD THIS LINE

# import calendar
import math

import psycopg2
# from sqlalchemy import create_engine, BigInteger
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger

# from dataclasses import dataclass
# from typing import Optional
 
# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- GBIF API Configuration ---
SHOVELS_BASE_URL = os.getenv('SHOVELS_BASE_URL')
SHOVELS_API_KEY = os.getenv('SHOVELS_API_KEY')
 
# --- Utility Functions ---
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=12),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True
)
def fetch_construction_permits(params):
    if not SHOVELS_BASE_URL or not SHOVELS_API_KEY:
        raise ValueError("SHOVELS_BASE_URL and SHOVELS_API_KEY must be set")

    url = f"{SHOVELS_BASE_URL}/permits/search"
    headers = {"X-API-Key": SHOVELS_API_KEY}

    
    start_date = str(f"{str(params['start_year'])-str(params['start_month'])-str(params['start_day'])}")
    end_date = str(f"{str(params['end_year'])-str(params['end_month'])-str(params['end_day'])}")

    final_params = {
        'geo_id': params['zip_code'], 
        'size': 15,
        'permit_from': start_date,
        'permit_to': end_date,
        'num_permits': params['num_permits'],
    }

    logger.info(f"Fetching from: {url} with params: {final_params}")

    response = requests.get(url, params=final_params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


# def extract_all_permits(zip_code=78701, max_records=1000):
#     """
#     Extracts ALL permits using pagination.
#     Loops until no more data or max_records reached.
#     """
#     all_records = []
#     offset = 0
#     page_size = 100
    
#     while len(all_records) < max_records:
#         # params = {
#         #     'zip_code': zip_code,
#         #     'num_permits': page_size,
#         #     'offset': offset  # This changes each loop
#         # }
#         params = {
#             'start_year': start_year,
#             'start_month': start_month,
#             'start_day': start_day,
#             'end_year': end_year,
#             'end_month': end_month,
#             'end_day': end_day,
#             'zip_code': zip_code,
#             'num_permits': num_permits
#         }
        
#         data = fetch_construction_permits(params)
#         records = data.get('items', [])
        
#         if not records:
#             break  # No more data
        
#         all_records.extend(records)
#         offset += len(records)
        
#         logger.info(f"Fetched {len(records)} records. Total: {len(all_records)}")
    
#     return all_records

# Simple version for daily ETL
def extract_permit_dataold2(zip_code=78701, num_permits=100):
    """Get the latest permits - one API call."""
    params = {'zip_code': zip_code, 'num_permits': num_permits}
    data = fetch_construction_permits(params)
    return data.get('items', [])

# CHQ: Claude AI included pagination 
def extract_permit_data_older(        
    start_year=2025,
    start_month=6,
    start_day=30,
    end_year=2026,
    end_month=1,
    end_day=24,
    zip_code=78701,
    num_permits=20,    
):
    """
    Extracts permit data from the Shovels API.
    Returns complete raw records for downstream transformation.
    """
    all_records = []
    offset = 0
    pages_fetched = 0
    num_pages_to_extract = 20
    
    params = {
        'start_year': start_year,
        'start_month': start_month,
        'start_day': start_day,
        'end_year': end_year,
        'end_month': end_month,
        'end_day': end_day,
        'zip_code': zip_code,
        'num_permits': num_permits
    }
    
    while pages_fetched < num_pages_to_extract:
        current_params = params.copy()
        current_params['offset'] = offset
        
        try:
            data = fetch_construction_permits(current_params)
            records = data.get('items', [])  # Based on your JSON structure
            
            if not records:
                logger.info("No more records available.")
                break
            
            all_records.extend(records)  # Keep all fields
            offset += len(records)
            pages_fetched += 1
            
            logger.info(f"Fetched page {pages_fetched}: {len(records)} records. Total: {len(all_records)}")
            
            # Check if there's a next page
            if data.get('next_cursor') is None:
                logger.info("Reached end of available data.")
                break
            
            time.sleep(0.5)  # Rate limiting
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {e}")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break
    
    logger.info(f"Extraction complete. Total records: {len(all_records)}")
    return all_records  # Return complete raw data
# CHQ: Claude AI generated function
def transform_permit_duration_old(raw_permits):
    """
    Transforms raw permit data for duration analysis.
    Returns a pandas DataFrame ready for analysis and loading.
    """
    if not raw_permits:
        return pd.DataFrame()
    
    # Convert to DataFrame first
    df = pd.DataFrame(raw_permits)
    
    # Filter out records with missing critical dates
    df = df.dropna(subset=['file_date', 'issue_date', 'final_date'])
    
    # Select key fields
    fields_to_keep = [
        'id', 'number', 'type', 'subtype', 'status',
        'file_date', 'issue_date', 'final_date',
        'approval_duration', 'construction_duration', 'total_duration',
        'property_type', 'job_value', 'contractor_id'
    ]
    
    df_clean = df[fields_to_keep].copy()
    
    # Rename for clarity
    df_clean.rename(columns={
        'id': 'permit_id',
        'number': 'permit_number',
        'type': 'permit_type',
        'subtype': 'permit_subtype'
    }, inplace=True)
    
    # Convert dates to datetime
    df_clean['file_date'] = pd.to_datetime(df_clean['file_date'])
    df_clean['issue_date'] = pd.to_datetime(df_clean['issue_date'])
    df_clean['final_date'] = pd.to_datetime(df_clean['final_date'])
    
    # Calculate derived metrics
    df_clean['approval_ratio'] = (
        df_clean['approval_duration'] / df_clean['total_duration']
    ).round(3)
    
    df_clean['construction_ratio'] = (
        df_clean['construction_duration'] / df_clean['total_duration']
    ).round(3)
    
    # Categorize duration
    df_clean['duration_category'] = pd.cut(
        df_clean['total_duration'],
        bins=[0, 30, 180, 365, float('inf')],
        labels=['Fast', 'Normal', 'Slow', 'Very Slow']
    )
    
    # Identify bottleneck phase
    df_clean['bottleneck_phase'] = df_clean.apply(
        lambda row: 'approval' if row['approval_duration'] > row['construction_duration'] else 'construction',
        axis=1
    )
    
    # Add extraction timestamp for audit trail
    df_clean['extracted_at'] = datetime.now()
    
    return df_clean


def transform_permit_duration(raw_permits):
    """
    Transforms raw permit data for duration analysis.
    Selects and derives only fields needed for this specific analysis.
    """
    transformed = []
    
    for permit in raw_permits:
        # Skip if missing critical fields
        if not all([permit.get('file_date'), permit.get('issue_date'), permit.get('final_date')]):
            continue
        
        # Select key fields + calculate derived metrics
        record = {
            # Key identifiers
            'permit_id': permit['id'],
            'permit_number': permit['number'],
            'permit_type': permit['type'],
            'permit_subtype': permit['subtype'],
            'status': permit['status'],
            
            # Dates
            'file_date': permit['file_date'],
            'issue_date': permit['issue_date'],
            'final_date': permit.get('final_date'),
            
            # Durations
            'approval_duration': permit.get('approval_duration'),
            'construction_duration': permit.get('construction_duration'),
            'total_duration': permit.get('total_duration'),
            
            # Derived metrics
            'approval_ratio': permit.get('approval_duration', 0) / permit.get('total_duration', 1) if permit.get('total_duration') else None,
            'construction_ratio': permit.get('construction_duration', 0) / permit.get('total_duration', 1) if permit.get('total_duration') else None,
            
            # Categorization
            'duration_category': categorize_duration(permit.get('total_duration')),
            'bottleneck_phase': 'approval' if permit.get('approval_duration', 0) > permit.get('construction_duration', 0) else 'construction',
            
            # Optional context fields
            'property_type': permit.get('property_type'),
            'job_value': permit.get('job_value'),
        }
        
        transformed.append(record)
    
    return transformed

def categorize_duration(days):
    """Categorize permit duration."""
    if days is None:
        return 'Unknown'
    if days < 30:
        return 'Fast'
    elif days < 180:
        return 'Normal'
    elif days < 365:
        return 'Slow'
    else:
        return 'Very Slow'

def load_data(df, conn_string, table_name="permit_durations"):
    """
    Loads the transformed DataFrame into the PostgreSQL database
    with correct column types for permit duration analysis.
    """
    if df.empty:
        logger.info("No data to load.")
        return
    
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.types import String, DateTime, Float, Integer, Numeric
        
        engine = create_engine(conn_string)
        logger.info(f"Attempting to load {len(df)} records into '{table_name}' table...")
        
        # Dtype mapping for transformed permit duration data
        dtype_mapping = {
            # Identifiers
            'permit_id': String(50),
            'permit_number': String(50),
            'permit_type': String(50),
            'permit_subtype': String(100),
            'status': String(50),
            'contractor_id': String(50),
            
            # Dates - use DateTime for proper date columns
            'file_date': DateTime,
            'issue_date': DateTime,
            'final_date': DateTime,
            
            # Durations - use Integer for day counts
            'approval_duration': Integer,
            'construction_duration': Integer,
            'total_duration': Integer,
            
            # Ratios - use Numeric for precision
            'approval_ratio': Numeric(5, 3),  # e.g., 0.123
            'construction_ratio': Numeric(5, 3),
            
            # Categories
            'duration_category': String(20),
            'bottleneck_phase': String(20),
            
            # Context fields
            'property_type': String(50),
            'job_value': Numeric(12, 2),  # For currency values
            
            # Audit
            'extracted_at': DateTime,
        }
        
        # Only include columns that exist in the DataFrame
        dtype_mapping_filtered = {
            col: dtype for col, dtype in dtype_mapping.items() 
            if col in df.columns
        }
        
        # Load data
        df.to_sql(
            table_name, 
            engine, 
            if_exists='append',  # Changed from 'replace' to preserve historical data
            index=False, 
            dtype=dtype_mapping_filtered
        )
        
        logger.info(f"Successfully loaded {len(df)} records into '{table_name}'.")
        
    except Exception as e:
        logger.error(f"Error loading data into database: {e}", exc_info=True)
        raise  # Re-raise to see full error
    
# CHQ: Gemini AI fixed function to pass parameters as a dictionary 
# Inside your load logic after the data is successfully saved to the new table
def register_date_in_inventory(engine, date_obj, table_name, count):
    # 1. Use named placeholders (:key)
    query = text("""
    INSERT INTO data_inventory (available_date, table_name, record_count)
    VALUES (:available_date, :table_name, :record_count)
    ON CONFLICT (available_date) DO UPDATE SET 
        table_name = EXCLUDED.table_name,
        record_count = EXCLUDED.record_count,
        processed_at = CURRENT_TIMESTAMP;
    """)
    
    with engine.begin() as conn:
        # 2. Pass as a DICTIONARY to satisfy SQLAlchemy 2.0
        conn.execute(query, {
            "available_date": date_obj,
            "table_name": table_name,
            "record_count": count
        })
 
# --- Main ETL Orchestration Function ---

 

# --- EXTRACT FUNCTION ---
def extract_permit_data(
    start_year="2025",
    start_month="06",
    start_day="30",
    end_year="2026",
    end_month="01",
    end_day="24",
    zip_code=78701,
    num_permits=100
):
    """Extract permits - single API call."""
    params = {
        'start_year': start_year,
        'start_month': start_month,
        'start_day': start_day,
        'end_year': end_year,
        'end_month': end_month,
        'end_day': end_day,
        'zip_code': zip_code,
        'num_permits': num_permits
    }
    data = fetch_construction_permits(params)
    records = data.get('items', [])
    
    logger.info(f"Extracted {len(records)} records")
    return records


# --- MAIN ETL FUNCTION ---
def construction_etl(start_year, start_month, start_day, 
                     end_year, end_month, end_day, 
                     zip_code, num_permits, conn_string):
    """
    Orchestrates the ETL process for permit data.
    """
    logger.info(f"Running ETL from {start_year}-{start_month}-{start_day} to {end_year}-{end_month}-{end_day}")
    logger.info("--- ETL process started ---")
    
    # EXTRACT
    logger.info("--- EXTRACT STEP ---")
    raw_data = extract_permit_data(
        start_year=start_year,
        start_month=start_month,
        start_day=start_day,
        end_year=end_year,
        end_month=end_month,
        end_day=end_day,
        zip_code=zip_code,
        num_permits=num_permits
    )
    
    if not raw_data:
        raise RuntimeError("ETL failed: no data extracted")
     
     
    # TRANSFORM
    logger.info("--- TRANSFORM STEP ---")
    transformed_data = transform_permit_duration(raw_data)
    df = pd.DataFrame(transformed_data)
    logger.info(f"Transformed {len(df)} records")
    
    if df.empty:
        logger.info("No data to load after transformation")
        return
    
    # LOAD
    logger.info("--- LOAD STEP ---")
    load_data(df, conn_string, "permit_durations")
    
    logger.info("--- ETL process finished ---")

def construction_etl_old(start_year, start_month, start_day, end_year, end_month, end_day, zip_code, num_permits, conn_string):
    """
    Orchestrates the ETL process for Monarch Butterfly data for a given month and year.
    """
    my_calendar ={
        1: "january",
        2: "february",
        3: "march",
        4: "april",
        5: "may",
        6: "june",
        7: "july",
        8: "august",
        9: "september",
        10: "october",
        11: "november",
        12: "december",
    }


    logger.info(f"\n\nRunning ETL from {start_year}-{start_month}-{start_day} to {end_year}-{end_month}-{end_day} \n")
    logger.info("--- ETL process started ---") 
    # start_date = datetime(year, month, 1)
    # # Calculate the last day of the month
    # if month == 12:
    #     end_date = datetime(year, 12, 31)
    # else:
    #     end_date = datetime(year, month + 1, 1) - timedelta(days=1)

    logger.info("\n\n\n--- EXTRACT STEP ---\n\n\n")

    # raw_data = extract_gbif_data(target_year=year, target_month=month, whole_month=True, limiting_page_count=True, num_pages_to_extract=10, records_limitation=42)
    # raw_data = extract_permit_data(target_year=year, target_month=month, whole_month=True, limiting_page_count=True, num_pages_to_extract=10)

    raw_data = extract_permit_data(
        start_year=start_year,
        start_month=start_month,
        start_day=start_day,
        end_year=end_year,
        end_month=end_month,
        end_day=end_day,
        zip_code=zip_code,
        num_permits=num_permits, 
        # records_limitation=None
    )
    if raw_data:
        logger.info("\n\n\n--- TRANSFORM STEP ---\n\n\n")
        transformed_data = transform_permit_duration(raw_data)
        df = pd.DataFrame(transformed_data)
        logger.info(f"Transformed {len(df)} records")
        
        if not df.empty:
            logger.info("\n\n\n--- LOAD STEP ---\n\n\n")
            load_data(df, conn_string, "permit_durations")
        else:
            logger.info("No data to load after transformation")

    else:
        logger.info("No raw data extracted. ETL process aborted.")

    logger.info("--- ETL process finished ---")

 

def test_api_endpoint():
    """Test if the basic API endpoint works."""
    import requests
    
    if not SHOVELS_BASE_URL or not SHOVELS_API_KEY:
        logger.error("API credentials not set")
        return False
    
    # Test 1: Minimal parameters
    url = f"{SHOVELS_BASE_URL}/permits/search"
    headers = {'X-API-Key': SHOVELS_API_KEY}
    
    test_cases = [
    {
        "geo_id": "78701",
        "size": 50,
        "permit_from": "2025-06-01",
        "permit_to": "2026-01-02",
        "num_permits": 1,
        "geo_id": "q8fdm_HmVcc"
    }
    ]
    
    for i, params in enumerate(test_cases, 1):
        logger.info(f"Test {i}: {params}")
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            logger.info(f"  Status: {response.status_code}")
            logger.info(f"  Response: {response.text[:200]}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"  Keys in response: {data.keys()}")
                logger.info(f"  SUCCESS! Use these params: {params}")
                return True
        except Exception as e:
            logger.error(f"  Error: {e}")
    
    return False



def permit_search_etl_scan(start_year, start_month, start_day, end_year, end_month, end_day, zip_code, num_permits, conn_string):
    """
    Orchestrates the ETL process for Construction data for a given month and year.
    """

    my_calendar = {
        1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
        7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december",
    }

    logger.info(f"\n\nRunning ETL for dates in range {start_year}-{start_month}-{start_day} to  {end_year}-{end_month}-{end_day} \n")
    logger.info("--- ETL process started ---")

    logger.info("\n\n\n--- EXTRACT STEP ---\n\n\n")

    raw_data = extract_permit_data(
        start_year=start_year,
        start_month=start_month,
        start_day=start_day,
        end_year=end_year,
        end_month=end_month,
        end_day=end_day,
        zip_code=zip_code,
        num_permits=num_permits, 
        # records_limitation=None
    )

    if raw_data:
        logger.info("\n\n\n--- TRANSFORM STEP ---\n\n\n")
        transformed_df = transform_gbif_data(raw_data)
        if not transformed_df.empty:
            logger.info("\n\n\n--- LOAD STEP ---\n\n\n")
            # Corrected line: pass the variables directly to the table name string

            table_name = ""

            if(day < 10):
                table_name = f"{my_calendar[month]}0{day}{year}" 
            else:
                table_name = f"{my_calendar[month]}{day}{year}" 
 
            load_data(transformed_df, conn_string, table_name)
            
            # 2. Register the completion in the inventory table
            engine = create_engine(conn_string)
            
            # Create a date object for the inventory
            date_obj = datetime(year, month, day).date()
            record_count = len(transformed_df)
            
            logger.info(f"Registering {date_obj} in data_inventory...")
            # register_date_in_inventory(engine, date_obj, table_name, record_count)
            register_date_in_inventory_as_df(engine, date_obj, table_name, record_count)
            
        else:
            logger.info("Transformed DataFrame is empty. No data to load.")
    else:
        logger.info("No raw data extracted. ETL process aborted.")

    logger.info("--- ETL process finished ---")
 