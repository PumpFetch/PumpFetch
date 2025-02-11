#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
import asyncio

# Configure logging to display timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("generic_processor")

# Database configuration: set your credentials via environment variables or replace with placeholder values.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool with a defined pool size.
connection_pool = pooling.MySQLConnectionPool(pool_name="generic_pool", pool_size=5, **dbconfig)

# State file used to store the last processed record ID.
STATE_FILE = 'state.json'

def save_state(last_id):
    """
    Save the last processed record ID to a JSON state file.
    
    This function writes a JSON object with the key "last_id" so the script can resume processing from the last record.
    
    :param last_id: The ID of the last processed record.
    """
    with open(STATE_FILE, 'w') as f:
        json.dump({"last_id": last_id}, f)

def load_state():
    """
    Load the last processed record ID from the state file.
    
    If the file exists, this function returns the last processed ID; otherwise, it returns 0.
    
    :return: The last processed record ID.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f).get("last_id", 0)
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return 0
    return 0

async def process_records(last_id):
    """
    Process new records from a generic source and insert duplicates into a generic target.
    
    This function retrieves new records from a source table (with IDs greater than last_id).
    For each record, it checks for duplicates based on a comparison of specific attributes.
    If duplicates exist (i.e. if another record with the same attribute values is found), the record is inserted into a target table.
    
    :param last_id: The last processed record ID.
    :return: The new last processed record ID.
    """
    conn = None
    cursor = None
    new_last_id = last_id
    try:
        conn = connection_pool.get_connection()
        # Create a cursor that returns results as dictionaries.
        cursor = conn.cursor(dictionary=True)
        # Retrieve new records from the source table.
        cursor.execute("""
            SELECT id, unique_key, attribute1, attribute2, created_at
            FROM source_table
            WHERE id > %s
            ORDER BY id ASC
        """, (last_id,))
        records = cursor.fetchall()
        if not records:
            return last_id

        # Update new_last_id to the highest ID from the fetched records.
        new_last_id = records[-1]['id']

        # Check each record for duplicates based on attribute1 or attribute2.
        for record in records:
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM source_table
                WHERE (attribute1 = %s OR attribute2 = %s) AND unique_key <> %s
            """, (record['attribute1'], record['attribute2'], record['unique_key']))
            result = cursor.fetchone()
            # If duplicates are found, insert the record into the target table.
            if result and result['cnt'] > 0:
                cursor.execute("""
                    INSERT IGNORE INTO target_table (unique_key, attribute1, attribute2, created_at)
                    VALUES (%s, %s, %s, NOW())
                """, (record['unique_key'], record['attribute1'], record['attribute2']))
                logger.info(f"Record inserted for unique_key {record['unique_key']}")
        conn.commit()
    except Error as e:
        logger.error(f"Error in process_records: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return new_last_id

async def main_loop():
    """
    Main loop that continuously processes new records.
    
    This function loads the last processed record ID from a state file, processes new records,
    saves the updated state, and then waits for 5 seconds before checking for new records again.
    """
    last_id = load_state()
    while True:
        last_id = await process_records(last_id)
        save_state(last_id)
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main_loop())
