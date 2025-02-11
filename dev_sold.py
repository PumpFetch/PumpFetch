#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
from datetime import datetime
import asyncio

# Configure logging: output the timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("data_processor")

# Database configuration: set your credentials via environment variables or replace the placeholders.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool with a moderate pool size.
connection_pool = pooling.MySQLConnectionPool(pool_name="generic_pool", pool_size=5, **dbconfig)

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
    except Exception as e:
        logger.error(f"Error saving state: {e}")

def load_state():
    """
    Load the last processed record ID from the JSON state file.

    If the state file exists, this function reads and returns the last processed ID.
    If the file does not exist or an error occurs, it returns 0.
    
    :return: The last processed record ID, or 0 if not found.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f).get("last_id", 0)
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return 0
    return 0

async def process_data(last_id):
    """
    Process new records from a generic source and insert matching records into a generic target.

    This function retrieves new records (with an ID greater than the given last_id) from a generic source.
    It then iterates over these records and, for each record that meets a specific condition (here, a
    transaction type of 'sell'), performs a lookup in another generic table. If the lookup condition is met,
    the function inserts a corresponding record into a generic target table.

    Finally, the function commits the changes and returns the new last processed record ID.
    
    :param last_id: The ID of the last processed record.
    :return: The new last processed record ID.
    """
    conn = None
    cursor = None
    new_last_id = last_id
    try:
        # Get a connection from the pool.
        conn = connection_pool.get_connection()
        # Create a cursor that returns rows as dictionaries.
        cursor = conn.cursor(dictionary=True)
        # Retrieve new records from the generic source table.
        cursor.execute("""
            SELECT id, mint, traderPublicKey, txType, solAmount, updated_at
            FROM source_table
            WHERE id > %s
            ORDER BY id ASC
        """, (last_id,))
        updates = cursor.fetchall()

        # If no new records are found, return the current last_id.
        if not updates:
            return last_id

        # Update the last processed ID using the ID of the last record retrieved.
        new_last_id = updates[-1]['id']

        # Iterate through each new record.
        for update in updates:
            # Process only records with a transaction type of 'sell'.
            if update['txType'] != 'sell':
                continue

            # Retrieve a matching record from a generic lookup table based on a specific field.
            cursor.execute("""
                SELECT traderPublicKey FROM lookup_table
                WHERE mint = %s
                LIMIT 1
            """, (update['mint'],))
            lookup_result = cursor.fetchone()

            # If the lookup result exists and meets the condition, insert a record into the generic target table.
            if lookup_result and lookup_result['traderPublicKey'] == update['traderPublicKey']:
                cursor.execute("""
                    INSERT IGNORE INTO target_table (mint, traderPublicKey, solAmount, txType, processed_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (update['mint'], update['traderPublicKey'], update['solAmount'], update['txType']))
                logger.info(f"✅ Record created for mint {update['mint']}")

        # Commit all changes to the database.
        conn.commit()
    except Error as e:
        logger.error(f"❌ Error in processing data: {e}")
    finally:
        # Close the cursor and connection to free resources.
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return new_last_id

async def main_loop():
    """
    Main asynchronous loop that continuously processes new records.

    This function loads the last processed record ID from a state file, processes new records,
    saves the updated ID back to the state file, and then waits for 5 seconds before repeating the process.
    """
    last_id = load_state()
    while True:
        last_id = await process_data(last_id)
        save_state(last_id)
        await asyncio.sleep(5)

if __name__ == "__main__":
    # Start the main asynchronous loop when the script is executed.
    asyncio.run(main_loop())
