#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
from datetime import datetime
import asyncio
import time

# Configure logging to display timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection configuration using environment variables or placeholder values.
dbconfig_generic = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool with a defined pool size.
try:
    connection_pool = pooling.MySQLConnectionPool(
        pool_name="generic_pool",
        pool_size=5,
        **dbconfig_generic
    )
    logger.info("Connection pool created successfully.")
except Error as e:
    logger.error(f"Error creating connection pool: {e}")
    exit(1)

# Generic state file used to store the last processed record ID.
STATE_FILE = 'state.json'

def save_state(last_id):
    """
    Save the last processed record ID to a JSON state file.
    
    This function writes a JSON object with the key "last_id" to a file.
    Storing this value allows the script to resume processing from where it left off.
    
    :param last_id: The ID of the last processed record.
    """
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_id": last_id}, f)
        logger.debug(f"State saved: {last_id}")
    except Exception as e:
        logger.error(f"Error saving state: {e}")

def load_state():
    """
    Load the last processed record ID from the state file.
    
    If the state file exists, this function reads and returns the last processed ID.
    If the file does not exist or an error occurs, it returns 0.
    
    :return: The last processed record ID, or 0 if not found.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                return data.get("last_id", 0)
        except Exception as e:
            logger.error(f"Error loading state: {e}")
    return 0

async def identify_and_store_records(last_id):
    """
    Identify and store records based on new entries in a generic source table.
    
    This function searches the source table (for records with ID greater than last_id)
    for user identifiers that appear in at least two new records. For each record associated
    with such a user, if the record is not already present in the target table, it is inserted.
    
    :param last_id: The last processed record ID.
    :return: The new last processed record ID.
    """
    conn = None
    cursor = None
    new_last_id = last_id
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()

        # Step 1: Retrieve all new records from the generic source table.
        query_source = """
            SELECT id, record_key, user_id, created_at
            FROM source_table
            WHERE id > %s
            ORDER BY id ASC
        """
        cursor.execute(query_source, (last_id,))
        new_records = cursor.fetchall()

        if not new_records:
            return last_id

        # Update new_last_id with the highest ID from the retrieved records.
        new_last_id = new_records[-1][0]

        # Group records by user_id.
        user_records = {}
        for rec in new_records:
            rec_id, record_key, user_id, created_at = rec
            user_records.setdefault(user_id, []).append(record_key)

        # For each user appearing in two or more records, insert new records into the target table.
        for user_id, record_keys in user_records.items():
            if len(record_keys) >= 2:
                for record_key in record_keys:
                    # Check if this record is already present in the target table.
                    cursor.execute("SELECT 1 FROM target_table WHERE record_key = %s", (record_key,))
                    if cursor.fetchone():
                        continue
                    # Insert the record into the target table.
                    insert_query = """
                        INSERT INTO target_table (record_key, user_id, created_at)
                        VALUES (%s, %s, NOW())
                    """
                    cursor.execute(insert_query, (record_key, user_id))
                    logger.info(f"Record added: record_key={record_key}, user_id={user_id}")
        conn.commit()
    except Error as e:
        logger.error(f"Error in identify_and_store_records: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
    return new_last_id

async def update_records_continuously():
    """
    Continuously update the target table by processing new records from the source table.
    
    This function loads the last processed record ID from the state file, then enters an
    infinite loop where it calls identify_and_store_records() to process new records. After
    processing, it saves the updated last processed ID back to the state file and waits for
    the remainder of a 5-second interval before repeating.
    """
    last_id = load_state()
    while True:
        start_time = time.time()
        try:
            last_id = await identify_and_store_records(last_id)
            save_state(last_id)
        except Exception as e:
            logger.error(f"Error during continuous update: {e}")
        # Ensure a 5-second interval between iterations.
        elapsed = time.time() - start_time
        await asyncio.sleep(5 - elapsed if elapsed < 5 else 0)

if __name__ == "__main__":
    try:
        asyncio.run(update_records_continuously())
    except KeyboardInterrupt:
        logger.info("Script terminated manually.")
    except Exception as e:
        logger.error(f"Unexpected error in main script: {e}")
