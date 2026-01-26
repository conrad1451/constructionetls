# etl_script.py

import os
import construction_permits_module
from datetime import datetime, timedelta

XATA_DB_CONSTRUCTION = os.getenv('XATA_DB_CONSTRUCTION')
SHOVELS_BASE_URL = os.getenv('SHOVELS_BASE_URL')
SHOVELS_API_KEY = os.getenv('SHOVELS_API_KEY')

 
if not XATA_DB_CONSTRUCTION:
    print("FATAL ERROR: XATA_DB_CONSTRUCTION environment variable is NOT SET.")
    # Exit gracefully or raise an error
    exit(1)

conn_string_xata = XATA_DB_CONSTRUCTION 

conn_string = conn_string_xata 

if __name__ == '__main__':
    # --- Example Usage for a specific month (e.g., June 2025) ---
    # For a real cron job, you might calculate year/month dynamically
    # For testing, let's use the month following the current month
    current_date = datetime.now()
    target_year = current_date.year
    target_month = current_date.month + 1
    if target_month > 12:
        target_month = 1
        target_year += 1
 

# June 30 2024 - Jan 24 2026
# construction_permits_module.construction_etl(2024, 6, 30, 2026, 1, 24, 78701, 10, conn_string)

# June 30 2025 - Jan 24 2026
construction_permits_module.construction_etl(2025, 6, 30, 2026, 1, 24, 78701, 10, conn_string)
