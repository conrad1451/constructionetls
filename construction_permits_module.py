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
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError # This includes 4xx and 5xx errors from the server
    )),
    reraise=True
) 
# --- NEW: Function to call the AI endpoint in batch mode ---
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError # This includes 4xx and 5xx errors from the AI server
    )),
    reraise=True # Re-raise the last exception after retries are exhausted
)

# CHQ: Gemini AI removed the endpoint parameter to function so endpoint is constructed from the params
def fetch_construction_permits(params):
    """
    Fetches data from the Shovels API with retry logic.
    """
    if not SHOVELS_BASE_URL:
        raise ValueError("SHOVELS_BASE_URL environment variable is not set.")
    
    endpoint = f"{SHOVELS_BASE_URL}/v2/permits/search"
    headers = {"X-API-Key": SHOVELS_API_KEY}
    
    logger.info(f"Attempting to fetch data from: {endpoint} with params: {params}")
    response = requests.get(endpoint, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

 

# --- Extraction Function ---
 
# CHQ: Claude AI included pagination 
def extract_permit_data(        
    start_year=2024,
    start_month=6,
    start_day=30,
    end_year=2026,
    end_month=1,
    end_day=24,
    zip_code=78701,
    num_permits=10,    
):
    """
    Extracts permit data from the Shovels API.
    Returns complete raw records for downstream transformation.
    """
    all_records = []
    offset = 0
    pages_fetched = 0
    num_pages_to_extract = 10
    
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
            data = fetch_construction_permits(SHOVELS_BASE_URL, current_params)
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
    
def load_data(df, conn_string, table_name="gbif_occurrences"):
    """
    Loads the transformed DataFrame into the PostgreSQL database
    using a predefined dtype map to ensure correct column types.
    """
    if df.empty:
        logger.info("No data to load.")
        return

    try:
        from sqlalchemy import create_engine
        from sqlalchemy.types import String, DateTime, Float, BigInteger, Integer, Date

        engine = create_engine(conn_string)

        logger.info(f"Attempting to load {len(df)} records into '{table_name}' table...")

        # NOTE: This line is no longer necessary as the dtype mapping will handle NaT
        # df['date_only'] = df['date_only'].astype(str).replace({'NaT': None})

        # CHQ: Corrected dtype mapping to use SQLAlchemy types
        dtype_mapping = {
            'gbifID': String,
            'datasetKey': String,
            'datasetName': String,
            'publishingOrgKey': String,
            'publishingOrganizationTitle': String,
            'eventDate': String,
            'eventDateParsed': DateTime,
            'scientificName': String,
            'vernacularName': String,
            'taxonKey': BigInteger,
            'kingdom': String,
            'phylum': String,
            'class': String,
            'order': String,
            'family': String,
            'genus': String,
            'species': String,
            'decimalLatitude': Float,
            'decimalLongitude': Float,
            'coordinateUncertaintyInMeters': Float,
            'countryCode': String,
            'stateProvince': String,

        count = data.get('count', 0)
        end_of_records = data.get('endOfRecords', True)
        offset += len(records) # Use len(records) to correctly advance offset
        pages_fetched += 1 # Increment page count

        logger.info(f"Fetched {len(records)} records. Total: {len(all_records)}. Next offset: {offset}. End of records: {end_of_records}")
    
        # CHQ: made a fix in monarch butterfly module - multiple pages should now be able to be obtained
        # CHQ: Gemini AI implemented limiting page count logic    
        # Implement limiting_page_count logic
        # if limiting_page_count is not None and pages_fetched >= num_pages_to_extract:
        #     logger.info(f"Reached limiting_page_count ({num_pages_to_extract}). Stopping extraction.")
 

        # Implement a small delay between GBIF API calls to be polite and avoid rate limits
        if not end_of_records and len(records) > 0:
                time.sleep(0.5) # Half a second delay
        elif len(records) == 0 and offset > 0: # If no records but offset is not 0, it indicates no more data
            end_of_records = True

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error during GBIF extraction: {e.response.status_code} - {e.response.text}")
        # break
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during GBIF extraction: {e}")
        # break
    except Exception as e:
        logger.error(f"An unexpected error occurred during GBIF extraction: {e}")
        # break


def construction_etl(start_year, start_month, start_day, end_year, end_month, end_day, zip_code, num_permits, conn_string):
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
        # transformed_df = transform_gbif_data(raw_data)
        # if not transformed_df.empty:
        logger.info("\n\n\n--- LOAD STEP ---\n\n\n")
            # load_data(transformed_df, conn_string, my_calendar[month] + " " + str(year))
            # load_data(transformed_df, conn_string, calendar.month_name[month] + " " + str(year))
        # else:
            # logger.info("Transformed DataFrame is empty. No data to load.")

        load_data()
    else:
        logger.info("No raw data extracted. ETL process aborted.")

    logger.info("--- ETL process finished ---")

 
    for chosen_day in range(day_start, day_end+1):
        monarch_etl_day_scan(year, month, chosen_day, conn_string) # For Jun 30 2025 # had 164 entries