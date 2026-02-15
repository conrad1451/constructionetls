# etl_script.py

import os
import logging
import sys 

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

logger = logging.getLogger(__name__)


# Add to your script before the ETL
if __name__ == "__main__":
    logger.info("Testing API endpoint...")
    if construction_permits_module.test_api_endpoint():
        logger.info("API test passed, running ETL...")
        # Run ETL
    else:
        logger.error("API test failed - fix endpoint/params first")
        sys.exit(1)

# June 30 2024 - Jan 24 2026
# construction_permits_module.construction_etl(2024, 6, 30, 2026, 1, 24, 78701, 10, conn_string)

# # June 30 2025 - Jan 24 2026
# construction_permits_module.construction_etl(2025, 6, 30, 2026, 1, 24, 78701, 10, conn_string)


# Jan 25 2026 - Feb 14 2026
construction_permits_module.construction_etl(2026, 1, 25, 2026, 2, 14, 78701, 10, conn_string)
