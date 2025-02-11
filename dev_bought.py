#!/usr/bin/env python3
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
import json
from datetime import datetime
import asyncio

# Configure logging: output timestamp, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("process_data")

# Database configuration using environment variables or placeholder values.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DATABASE")
}

# Create a connection pool with a moderate pool size.
connection_pool = pooling.MySQLConnectionPool(
    pool_name="generic_pool",
    pool_size=5,
    **dbconfig
)

# State file to store the last processed record ID.
STATE_FILE = 'state.json'

def save_state(last_id):
    """
    Save the last processed record ID to a JSON state file.
    
    This function writes a JSON object with the key "last_id" to a file.
    This allows the script to resume processing from where it left off.
    
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
    Process new records from a source table and insert matching records into a target table.
    
    This function retrieves up to 500 new records (with an ID greater than the given last_id)
    from a source table. For each record with a transaction type of 'buy', it performs a lookup
    in another table to verify a condition. If the condition is met, the record is inserted into
    a target table. Finally, the function commits the changes and returns the new last processed ID.
    
    :param last_id: The ID of the last processed record.
    :return: The new last processed record ID.
    """
    conn = None
    cursor = None
    new_last_id = last_id
    try:
        # Get a database connection from the pool.
        conn = connection_pool.get_connection()
        # Create a cursor that returns query results as dictionaries.
        cursor = conn.cursor(dictionary=True)
        # Retrieve new records from the source table (use a generic table name).
        cursor.execute("""
            SELECT id, mint, traderPublicKey, txType, solAmount, updated_at
            FROM source_table
            WHERE id > %s
            ORDER BY id ASC
            LIMIT 500
        """, (last_id,))
        records = cursor.fetchall()

        # If no new records are found, return the current last_id.
        if not records:
            return last_id

        # Update the last processed ID.
        new_last_id = records[-1]['id']

        # Process each record.
        for record in records:
            # Only process records with a transaction type of 'buy'.
            if record['txType'] != 'buy':
                continue

            # Perform a lookup in another table to retrieve a matching value based on 'mint'.
            cursor.execute("""
                SELECT traderPublicKey FROM lookup_table
                WHERE mint = %s
                LIMIT 1
            """, (record['mint'],))
            lookup = cursor.fetchone()

            # If the lookup value matches, insert a corresponding record into the target table.
            if lookup and lookup['traderPublicKey'] == record['traderPublicKey']:
                cursor.execute("""
                    INSERT IGNORE INTO target_table (mint, traderPublicKey, solAmount, txType, processed_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (record['mint'], record['traderPublicKey'], record['solAmount'], record['txType']))
                logger.info(f"✅ Record processed for mint {record['mint']}")
        
        # Commit the transaction.
        conn.commit()
    except Error as e:
        logger.error(f"❌ Error in processing data: {e}")
    finally:
        # Close the cursor and connection to free up resources.
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return new_last_id

async def main_loop():
    """
    Main asynchronous loop that continuously processes new records.
    
    This loop loads the last processed record ID, processes new records,
    saves the updated ID back to the state file, and then waits for 5 seconds
    before checking for new records again.
    """
    last_id = load_state()
    while True:
        last_id = await process_data(last_id)
        save_state(last_id)
        await asyncio.sleep(5)

if __name__ == "__main__":
    # Start the main asynchronous loop when the script is executed.
    asyncio.run(main_loop())
