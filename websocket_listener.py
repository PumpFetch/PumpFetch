#!/usr/bin/env python3
import asyncio
import websockets
import json
import mysql.connector
from mysql.connector import pooling, Error
import logging
import os
from datetime import datetime, timedelta
import random

# Configure logging to include timestamp, module name, log level, and message.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration: use environment variables or default placeholder values.
dbconfig = {
    "host": os.getenv('DB_HOST', "YOUR_DB_HOST"),
    "user": os.getenv('DB_USER', "YOUR_DB_USER"),
    "password": os.getenv('DB_PASSWORD', "YOUR_DB_PASSWORD"),
    "database": os.getenv('DB_DATABASE', "YOUR_DB_NAME")
}

# Create a connection pool with a defined pool size.
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    **dbconfig
)

# Global variables to manage the WebSocket connection and track subscription state.
websocket_instance = None
subscribed_tokens = set()
messages_sent = 0
messages_received = 0

async def subscribe_token_updates():
    """
    Establish and maintain a persistent WebSocket connection for receiving updates.

    This function attempts to connect to a WebSocket server (using a URI from the environment or a default placeholder).
    Once connected, it sends a subscription request for new records. It then continuously listens for incoming messages,
    processes each message based on its type, and implements reconnection logic with exponential backoff in case of errors.
    """
    global websocket_instance, messages_sent, messages_received
    # Retrieve the WebSocket URI from the environment or use a default placeholder.
    uri = os.getenv('WEBSOCKET_URI', "wss://your-websocket-uri/api/data")
    max_retries = 10
    retry_count = 0

    # Loop until the maximum number of connection retries is reached.
    while retry_count < max_retries:
        try:
            websocket_instance = await websockets.connect(uri, ping_interval=20, close_timeout=10)
            logger.info("WebSocket connected for receiving updates.")
            # Send a subscription request for new records.
            await websocket_instance.send(json.dumps({"method": "subscribeNewToken"}))
            messages_sent += 1
            logger.info(f"Subscription request sent. Total messages sent: {messages_sent}")

            # Continuously listen for incoming messages.
            while True:
                try:
                    message = await websocket_instance.recv()
                    messages_received += 1
                    data = json.loads(message)
                    tx_type = data.get("txType")
                    # Process the message based on its transaction type.
                    if tx_type == "create":
                        await handle_new_token(data)
                    elif tx_type in ("sell", "buy"):
                        await save_token_update(data)
                    else:
                        if data.get("method") == "newToken":
                            await handle_new_token(data)
                        elif "mint" in data:
                            await save_token_update(data)
                except websockets.exceptions.ConnectionClosedError as e:
                    logger.warning(f"WebSocket connection closed: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except websockets.exceptions.ConnectionClosedError as e:
            if websocket_instance:
                await websocket_instance.close()
                logger.info("WebSocket closed.")
                websocket_instance = None
            # Wait a short period before attempting reconnection.
            await asyncio.sleep(5)
            for i in range(5, 0, -1):
                logger.info(f"Reconnecting in {i} seconds...")
                await asyncio.sleep(1)
            retry_count += 1
            # Apply exponential backoff with a slight random delay.
            backoff_time = min(60, (2 ** retry_count) + random.uniform(0, 1))
            logger.warning(f"Retrying connection in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            await asyncio.sleep(5)
    else:
        logger.error("Maximum connection retries reached. Ending task.")

    if websocket_instance:
        await websocket_instance.close()
        websocket_instance = None
        logger.info("WebSocket closed.")

async def handle_new_token(data):
    """
    Process a notification of a new record.

    When a new record is detected, this function checks if it has already been processed (using the 'mint' field).
    If not, it adds the identifier to the subscription set, saves the new record to a generic primary table,
    and sends a request to subscribe to further updates related to the record.
    
    :param data: The JSON data received from the WebSocket message.
    """
    global subscribed_tokens, messages_sent
    mint = data.get("mint")
    if mint and mint not in subscribed_tokens:
        subscribed_tokens.add(mint)
        await save_to_primary_table(data)
        await subscribe_token(mint)
        messages_sent += 1
        logger.info(f"New record subscribed: {mint}. Total messages sent: {messages_sent}")

async def save_to_primary_table(data):
    """
    Save a new record to the generic primary table in the database.

    This function inserts a new record into a generic primary table using data received from the WebSocket.
    It employs an INSERT IGNORE statement to prevent duplicate entries and commits the transaction after execution.
    
    :param data: The data containing the record details.
    """
    conn = None
    cursor = None
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT IGNORE INTO primary_table 
            (signature, mint, traderPublicKey, txType, initialBuy, solAmount, bondingCurveKey, 
             vTokensInPrimary, vSolInPrimary, marketCap, name, symbol, uri, pool, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            data.get("signature"),
            data.get("mint"),
            data.get("traderPublicKey"),
            data.get("txType"),
            data.get("initialBuy"),
            data.get("solAmount"),
            data.get("bondingCurveKey"),
            data.get("vTokensInBondingCurve"),
            data.get("vSolInBondingCurve"),
            data.get("marketCapSol"),
            data.get("name"),
            data.get("symbol"),
            data.get("uri"),
            data.get("pool")
        ))
        conn.commit()
        logger.info(f"Record saved: {data.get('mint')}")
    except Exception as e:
        logger.error(f"Error saving record: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def subscribe_token(mint):
    """
    Send a subscription request for further updates related to a specific record.

    This function sends a request via the WebSocket to subscribe to trade updates associated with the provided record.
    It increments the global message counter once the request is sent.
    
    :param mint: The identifier for the record.
    """
    global messages_sent
    if websocket_instance:
        await websocket_instance.send(json.dumps({
            "method": "subscribeTokenTrade",
            "keys": [mint]
        }))
        messages_sent += 1
        logger.info(f"Trade subscription sent for record {mint}. Total messages sent: {messages_sent}")

async def save_token_update(data):
    """
    Save an update for a record to the generic update table in the database.

    This function inserts an update for a record into a generic update table using the provided data.
    After executing the insert query, it commits the transaction.
    
    :param data: The data containing update details.
    """
    conn = None
    cursor = None
    try:
        conn = connection_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO update_table 
            (mint, traderPublicKey, txType, solAmount, vTokensInPrimary, vSolInPrimary, marketCap, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            data.get("mint"),
            data.get("traderPublicKey"),
            data.get("txType"),
            data.get("solAmount"),
            data.get("vTokensInBondingCurve"),
            data.get("vSolInBondingCurve"),
            data.get("marketCapSol")
        ))
        conn.commit()
        logger.info(f"Update saved for record: {data.get('mint')}")
    except Exception as e:
        logger.error(f"Error saving update: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

async def clean_update_table():
    """
    Periodically remove outdated records from the generic update table.

    This function runs indefinitely, deleting records from the update table that are older than 10 minutes.
    It logs the number of records removed and then waits for 10 minutes before the next cleanup cycle.
    """
    while True:
        conn = None
        cursor = None
        try:
            conn = connection_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM update_table 
                WHERE updated_at < %s
            """, (datetime.now() - timedelta(minutes=10),))
            deleted = cursor.rowcount
            conn.commit()
            if deleted > 0:
                logger.info(f"{deleted} outdated records removed from update_table.")
        except Exception as e:
            logger.error(f"Error cleaning update table: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            await asyncio.sleep(600)  # Pause for 10 minutes before the next cleanup.

async def check_and_unsubscribe_tokens():
    """
    Periodically check for records that should be unsubscribed and perform the unsubscription.

    This function runs indefinitely, querying for records in the generic primary table that are older than a defined period
    (e.g., 1 hour). For each record found, if it is currently subscribed, the function sends an unsubscription request,
    removes it from the subscription set, and logs the action.
    """
    global subscribed_tokens, messages_sent
    while True:
        conn = None
        cursor = None
        try:
            conn = connection_pool.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT mint FROM primary_table WHERE created_at < %s
            """, (datetime.now() - timedelta(hours=1),))
            records_to_unsubscribe = {row['mint'] for row in cursor.fetchall()}
            for mint in records_to_unsubscribe & subscribed_tokens:
                await unsubscribe_token(mint)
                subscribed_tokens.remove(mint)
                messages_sent += 1
                logger.info(f"Unsubscription sent for record {mint}. Total messages sent: {messages_sent}")
        except Exception as e:
            logger.error(f"Error checking unsubscriptions: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            await asyncio.sleep(60)

async def unsubscribe_token(mint):
    """
    Send an unsubscription request for a specific record.

    This function sends a request via the WebSocket to unsubscribe from further updates related to the record.
    It increments the global message counter after sending the request.
    
    :param mint: The identifier for the record.
    """
    global messages_sent
    if websocket_instance:
        await websocket_instance.send(json.dumps({
            "method": "unsubscribeTokenTrade",
            "keys": [mint]
        }))
        messages_sent += 1
        logger.info(f"Unsubscription sent for record {mint}. Total messages sent: {messages_sent}")

async def main():
    """
    Main asynchronous entry point for processing real-time updates.

    This function concurrently starts tasks for subscribing to updates, checking for unsubscriptions, and cleaning
    outdated records. It waits for all tasks to complete using asyncio.gather.
    """
    subscribe_task = asyncio.create_task(subscribe_token_updates())
    unsubscribe_task = asyncio.create_task(check_and_unsubscribe_tokens())
    cleanup_task = asyncio.create_task(clean_update_table())
    await asyncio.gather(subscribe_task, unsubscribe_task, cleanup_task)
    logger.info(f"Processing complete. Messages sent: {messages_sent}, Messages received: {messages_received}")

if __name__ == "__main__":
    asyncio.run(main())
