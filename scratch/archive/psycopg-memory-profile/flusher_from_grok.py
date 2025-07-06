"""
https://grok.com/chat/7ffdad24-37f2-4afa-a5d4-692907ff1078
"""
import asyncio
import psycopg

async def create_pool():
    """Create an asynchronous connection pool for PostgreSQL."""
    return await psycopg.AsyncConnectionPool(
        conninfo="host=localhost port=5432 dbname=your_database user=your_username password=your_password",
        min_size=5,
        max_size=10
    )

async def flusher(queue, max_size, interval, pool):
    """Flush transactions from the queue to the database periodically or when full."""
    flush_event = asyncio.Event()
    while True:
        # Wait for either the flush event (queue full) or the interval (0.5 seconds)
        done, pending = await asyncio.wait(
            [flush_event.wait(), asyncio.sleep(interval)],
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Collect all current items from the queue
        items = []
        while True:
            try:
                item = queue.get_nowait()
                items.append(item)
            except asyncio.QueueEmpty:
                break
        
        # If there are items, flush them to the database
        if items:
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Batch insert transactions
                    await cur.executemany("INSERT INTO transactions (data) VALUES (%s)", [(item,) for item in items])
                await conn.commit()
        
        # Clear the flush event if it was set
        if flush_event in done:
            flush_event.clear()

async def add_transaction(queue, transaction, max_size, flush_event):
    """Add a transaction to the queue and signal flush if the queue is full."""
    await queue.put(transaction)
    if queue.qsize() >= max_size:
        flush_event.set()

async def main():
    """Main function to set up and run the buffer logic."""
    # Initialize the database connection pool
    pool = await create_pool()
    
    # Initialize the queue and parameters
    queue = asyncio.Queue()
    max_size = 100  # Maximum number of transactions before flushing
    interval = 0.5  # Flush interval in seconds
    flush_event = asyncio.Event()
    
    # Start the flusher task
    flusher_task = asyncio.create_task(flusher(queue, max_size, interval, pool))
    
    # Simulate adding transactions
    for i in range(150):  # Add 150 transactions to test both conditions
        await add_transaction(queue, f"Transaction {i}", max_size, flush_event)
        await asyncio.sleep(0.01)  # Simulate some delay between transactions
    
    # Wait a bit to ensure all transactions are flushed
    await asyncio.sleep(1)
    
    # Clean up
    flusher_task.cancel()
    try:
        await flusher_task
    except asyncio.CancelledError:
        pass
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())