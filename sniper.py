#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
from datetime import datetime, timedelta
import asyncio

# Configure logging: display timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("generic_processor")

# Database configuration: use environment variables or placeholder values.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool.
connection_pool = pooling.MySQLConnectionPool(pool_name="generic_pool", pool_size=5, **dbconfig)

# State file to store the last processed timestamp.
STATE_FILE = 'state_time.json'

def save_state(last_time):
    """
    Save the last processed timestamp to a JSON state file.

    :param last_time: A datetime object representing the last processed timestamp.
    """
    data = {"last_time": last_time.isoformat()}
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving state: {e}")

def load_state():
    """
    Load the last processed timestamp from the state file.

    :return: A datetime object representing the last processed timestamp.
             If the state file cannot be loaded, return (now - 10 minutes).
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
                return datetime.fromisoformat(data["last_time"])
        except Exception as e:
            logger.error(f"Error loading state: {e}")
    # On first run, set state to (now - 10 minutes)
    return datetime.now() - timedelta(minutes=10)

async def process_records():
    """
    Process new records from a generic source table that were created within the last 10 minutes.
    
    For each new record, a 5-second window (from created_at to created_at + 5 seconds) is defined.
    Within that window, any transactions (of type 'buy' or 'sell') for the same unique key are fetched
    from a generic updates table and inserted into a generic target table.
    
    :return: The timestamp (created_at) of the last processed record.
    """
    last_time = load_state()
    conn = None
    cursor = None
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor(dictionary=True)
        now = datetime.now()
        # Consider only records created within the last 10 minutes.
        cutoff = now - timedelta(minutes=10)
        # Retrieve new records from the generic source table.
        cursor.execute("""
            SELECT id, unique_key, created_at
            FROM source_table
            WHERE created_at > %s AND created_at >= %s
            ORDER BY created_at ASC
            LIMIT 100
        """, (last_time, cutoff))
        records = cursor.fetchall()
        if records:
            for record in records:
                unique_key = record['unique_key']
                created_at = record['created_at']
                logger.info(f"Processing record {unique_key} (created_at: {created_at})")
                window_end = created_at + timedelta(seconds=5)
                # If current time is before the end of the window, wait until window_end.
                now2 = datetime.now()
                if now2 < window_end:
                    sleep_duration = (window_end - now2).total_seconds()
                    logger.info(f"Waiting {sleep_duration:.2f} seconds for record {unique_key}")
                    await asyncio.sleep(sleep_duration)
                # Search the generic updates table for transactions for this unique key within the window.
                cursor.execute("""
                    SELECT id, unique_key, user_id, txType, amount, updated_at
                    FROM updates_table
                    WHERE unique_key = %s
                      AND updated_at BETWEEN %s AND %s
                      AND txType IN ('buy', 'sell')
                """, (unique_key, created_at, window_end))
                updates = cursor.fetchall()
                if updates:
                    for update in updates:
                        cursor.execute("""
                            INSERT INTO target_table (unique_key, user_id, amount, txType, processed_at)
                            VALUES (%s, %s, %s, %s, NOW())
                        """, (update['unique_key'], update['user_id'], update['amount'], update['txType']))
                        logger.info(f"Inserted target record for {unique_key} (txType: {update['txType']})")
                else:
                    logger.info(f"No transactions found in the 5-second window for record {unique_key}.")
                # Update last_time to the created_at of the current record.
                last_time = record['created_at']
            conn.commit()
        else:
            logger.info("No new records found created within the last 10 minutes.")
    except Error as e:
        logger.error(f"Error in process_records: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    save_state(last_time)
    return last_time

async def main_loop():
    """
    Main loop that continuously processes new records.

    This function repeatedly calls process_records() and then waits 1 second before starting the next iteration.
    """
    while True:
        await process_records()
        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Script terminated manually.")
