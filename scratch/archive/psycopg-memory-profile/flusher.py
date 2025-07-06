import numpy as np
import asyncio

entire_list = []
async def flusher(queue, max_queue_size, interval, flush_event, pool):
    while True:
        done, pending = await asyncio.wait(
            [flush_event.wait(), asyncio.sleep(interval)],
            return_when=asyncio.FIRST_COMPLETED
        )

        mylist = []
        while True:
            try:
                item = queue.get_nowait()
                mylist.append(item)
            except asyncio.QueueEmpty:
                break

        print(flush_event.is_set(),len(mylist),len(entire_list))
        if len(mylist) > 0:
            entire_list.extend(mylist) # for assert
            # TODO
            # async with pool.connection() as conn:
            #     async with conn.cursor() as cur:
            #         await cur.executemany(...)
            #     await conn.commit()

        # clear flush event if it was set
        if flush_event.is_set():
            flush_event.clear()

async def add_transaction(queue, transaction, max_queue_size, flush_event):
    await queue.put(transaction)
    if queue.qsize() >= max_queue_size:
        flush_event.set()

async def main():
    total_event_count = 1500
    pool = None # TODO

    queue = asyncio.Queue()
    max_queue_size = 50 
    interval = 0.5  # flush interval in seconds

    flush_event = asyncio.Event()

    flusher_task = asyncio.create_task(flusher(queue, max_queue_size, interval, flush_event, pool))
    
    # mockup transactionss
    for i in range(total_event_count):

        await add_transaction(queue, f"Transaction {i}", max_queue_size, flush_event)

        num = np.random.randint(1,4) if np.random.rand() < 0.01 else 0.001
        await asyncio.sleep(num)
    
    # Wait a bit to ensure all transactions are processed
    await asyncio.sleep(5)
    
    # clean up
    flusher_task.cancel()
    try:
        await flusher_task
    except asyncio.CancelledError:
        pass

    #if pool is not None:
    #    await pool.close()

    assert(len(entire_list)==total_event_count)
    print("pass")

if __name__ == "__main__":
    asyncio.run(main())


"""
https://gist.github.com/pangyuteng/91323f79aa15bff8603cad6d96aaecb3

buffer flushing logic using python asyncio Queue and Event

keywords: buffer flushing, producer consumer

"""