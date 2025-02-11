#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
from datetime import datetime
import asyncio

# Configure logging to display the timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("generic_wallet_processor")

# Database configuration: use environment variables or replace with your placeholder values.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool with a specified pool size.
connection_pool = pooling.MySQLConnectionPool(
    pool_name="generic_pool",
    pool_size=5,
    **dbconfig
)

# State file to store the last processed record ID from the source table.
STATE_FILE = 'state.json'

def save_state(last_id):
    """
    Save the last processed record ID to a JSON state file.

    This function writes a JSON object containing the key "last_id" to a file.
    Storing this value allows the script to resume processing from the last processed record.
    
    :param last_id: The ID of the last processed record.
    """
    data = {"last_id": last_id}
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving state: {e}")

def load_state():
    """
    Load the last processed record ID from the state file.

    If the state file exists and is valid, this function returns the stored record ID.
    Otherwise, it returns 0, indicating that processing should start from the beginning.
    
    :return: The last processed record ID, or 0 if not available.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f).get("last_id", 0)
        except Exception as e:
            logger.error(f"Error loading state: {e}")
    return 0

async def process_wallets():
    """
    Process new records from the source table and insert target records for users with duplicate entries.

    This function performs the following steps:
      1. Loads the last processed record ID.
      2. Retrieves new entries from the source table (records with id > last_id).
      3. Updates the new_last_id based on the latest retrieved record.
      4. Determines which user IDs appear more than once in the entire source table.
      5. For each new record whose user ID is among the duplicate set, an entry is inserted
         into the target table. This allows you to detect if a user (wallet) is performing repeated transactions.
      6. Commits the transaction and saves the new state.

    :return: The new last processed record ID.
    """
    last_id = load_state()
    conn = None
    cursor = None
    new_last_id = last_id
    try:
        # Get a connection from the pool and create a cursor that returns rows as dictionaries.
        conn = connection_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Retrieve new entries from the source table that have not yet been processed.
        cursor.execute("""
            SELECT *
            FROM source_table
            WHERE id > %s
            ORDER BY id ASC
            LIMIT 100
        """, (last_id,))
        new_rows = cursor.fetchall()
        if not new_rows:
            return last_id
        
        # Update new_last_id with the ID of the last retrieved record.
        new_last_id = new_rows[-1]['id']
        
        # Retrieve all user IDs from the source table that occur more than once.
        cursor.execute("""
            SELECT user_id
            FROM source_table
            GROUP BY user_id
            HAVING COUNT(*) > 1
        """)
        duplicate_users = {row['user_id'] for row in cursor.fetchall()}
        
        # For each new record whose user_id is in the duplicate set,
        # insert a corresponding entry into the target table.
        for row in new_rows:
            if row['user_id'] in duplicate_users:
                cursor.execute("""
                    INSERT INTO target_table (unique_key, user_id, amount, transaction_type, processed_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (row['unique_key'], row['user_id'], row['amount'], row['transaction_type']))
                logger.info(f"Inserted target record for user {row['user_id']} (unique_key: {row['unique_key']}).")
        conn.commit()
    except Error as e:
        logger.error(f"Error in process_wallets: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    # Save the new state after processing.
    save_state(new_last_id)
    return new_last_id

async def main_loop():
    """
    Main asynchronous loop that continuously processes new records.

    This function repeatedly calls process_wallets() to process new batches of records and then
    pauses for a few seconds before the next iteration.
    """
    while True:
        await process_wallets()
        # Wait for 5 seconds before processing the next batch.
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Script terminated manually.")
