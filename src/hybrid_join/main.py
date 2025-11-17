# Queue

# ETL

# Hybrid Join
# Key Components: hash table, queue, disk buffer, and a stream buffer.

"""
Disk Buffer: A memory buffer that holds a loaded partition p of size vP from R. This is
part of the "Join Window." Each disk partition size will be 500 tuples.

Stream Input (w): The number of available slots in the hash table which are equal to the
number of free up spaces in the previous iteration.

The stream buffer will continuously get transactional data as the HYBRIDJOIN is
for the near real time data. A thread will be implemented which will continuously get data
from the transactional_data.csv provided into the stream buffer independent of the join
operation. Then there will be another thread which will implement the HYBRIDJOIN
algorithm.

"""
import pandas as pd
from stream_buffer import StreamBuffer
from hash_table import HashTable
from queue import Queue
import threading
import time

def generate_tuple(df: pd.DataFrame, index: int) -> tuple:
    """Return a tuple for each transactional row"""

    row = df.iloc[index]

    orderID = int(row.orderID)
    Customer_ID = int(row.Customer_ID)
    Product_ID = str(row.Product_ID)
    quantity = int(row.quantity)
    date = str(row.date)

    trans_tuple = (orderID, Customer_ID, Product_ID, quantity, date)

    return trans_tuple

def extract_key(tup: tuple) -> int:
    # Extracting Key (CustomerID) for Hashing
    _, key, *_ = tup
    return key

def stream_feeder(stream_buffer: StreamBuffer, csv_path: str) -> None:
    """
    Continuously read the CSV and push tuples into stream buffer.
    Simulates a real-time transactional stream.
    """

    df = pd.read_csv(csv_path)
    idx = 0

    while True:
        if idx < len(df):
            row_tuple: tuple = generate_tuple(df, idx)
            stream_buffer.push(row_tuple)
            idx += 1 

def hybridjoin_worker(stream_buffer: StreamBuffer, hash_table: HashTable, queue: Queue) -> None:
    """
    Continuously runs the Hybrid Join algorithm.
    """

    while True:

        # Check available hash table slots
        slots_available = hash_table.get_available_slots()

        # Load up to w stream tuples
        loaded = 0
        while loaded < slots_available and not stream_buffer.is_empty(): 
            row: tuple = stream_buffer.pop() 
            key = extract_key(row)  # Customer_ID 

            hash_table.insert(key, row) 
            queue.enqueue(key) 

            loaded += 1

        # 3. Get oldest key
        oldest_key = queue.dequeue() 
        if oldest_key is None:
            time.sleep(0.001)
            continue

        # 4. Load disk partition for that key (pseudo)
        # partition = load_partition(oldest_key)
        # probe partition with hash table...

        time.sleep(0.001)  # loop pacing

if __name__=="__main__":
    
    DATA_PATH = '../../data/transactional_data.csv'
    df = pd.read_csv(DATA_PATH)

    stream_buffer = StreamBuffer()
    hash_table = HashTable()
    queue = Queue() 

    # Create threads
    feeder_thread = threading.Thread(
        target=stream_feeder, args=(stream_buffer, DATA_PATH), daemon=True
    )
    join_thread = threading.Thread(
        target=hybridjoin_worker, args=(stream_buffer, hash_table, queue), daemon=True
    ) 
    
    # Start threads
    feeder_thread.start()
    join_thread.start()

    # Keep main thread alive
    while True:
        time.sleep(1)




        
        
      

    