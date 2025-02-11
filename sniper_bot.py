#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
from collections import defaultdict
from datetime import datetime
import asyncio

# Configure logging to display the timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("generic_bot")

# Database configuration: use environment variables or placeholder values.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool.
connection_pool = pooling.MySQLConnectionPool(pool_name="generic_pool", pool_size=5, **dbconfig)

# State file used to store the last processed record ID from the source table.
STATE_FILE = 'state.json'

# Configurable threshold – e.g., if a wallet with a given amount has at least 2 entries.
THRESHOLD = 2

def save_state(last_id):
    """
    Save the last processed record ID to a JSON state file.
    
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
    
    :return: The last processed record ID, or 0 if not found.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f).get("last_id", 0)
        except Exception as e:
            logger.error(f"Error loading state: {e}")
    return 0

async def process_records():
    """
    Read all new entries from the source table (with id > last_id), group them by (user_id, amount),
    and if a wallet in a group meets the threshold, query additional token information from a lookup table
    based on the unique key and insert all details into a target table.
    
    :return: The ID of the last processed record.
    """
    last_id = load_state()
    conn = None
    cursor = None
    new_last_id = last_id
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        # Retrieve new entries from the source table that have not yet been processed.
        cursor.execute("""
            SELECT id, unique_key, user_id, amount, transaction_type, processed_at
            FROM source_table
            WHERE id > %s
            ORDER BY id ASC
            LIMIT 500
        """, (last_id,))
        new_rows = cursor.fetchall()
        if not new_rows:
            return last_id
        new_last_id = new_rows[-1]['id']
        # Group records by (user_id, amount)
        groups = defaultdict(list)
        for row in new_rows:
            try:
                amt = float(row['amount']) if row['amount'] is not None else 0.0
            except Exception as e:
                logger.error(f"Error converting amount: {e}")
                amt = 0.0
            key = (row['user_id'], amt)
            groups[key].append(row)
        
        # For each group that meets the threshold, insert all corresponding rows into the target table.
        for key, rows in groups.items():
            if len(rows) >= THRESHOLD:
                user_id, amt = key
                for row in rows:
                    unique_key = row['unique_key']
                    # Query additional token information from the lookup table based on the unique key.
                    cursor.execute("""
                        SELECT token_name, token_symbol
                        FROM lookup_table
                        WHERE unique_key = %s
                        LIMIT 1
                    """, (unique_key,))
                    token_info = cursor.fetchone()
                    token_name = token_info['token_name'] if token_info and 'token_name' in token_info else None
                    token_symbol = token_info['token_symbol'] if token_info and 'token_symbol' in token_info else None
                    cursor.execute("""
                        INSERT INTO target_table (unique_key, user_id, amount, transaction_type, processed_at, token_name, token_symbol)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (unique_key, user_id, amt, row['transaction_type'], row['processed_at'], token_name, token_symbol))
                    logger.info(f"Target record inserted: Wallet {user_id}, amount {amt}, unique_key {unique_key}, token_name {token_name}, token_symbol {token_symbol} (transaction_type: {row['transaction_type']})")
        conn.commit()
    except Error as e:
        logger.error(f"Error in process_records: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    save_state(new_last_id)
    return new_last_id

async def main_loop():
    """
    Main loop that continuously processes new records.
    
    This function repeatedly calls process_records() and then waits for 5 seconds before starting the next iteration.
    """
    while True:
        await process_records()
        await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Script terminated manually.")
