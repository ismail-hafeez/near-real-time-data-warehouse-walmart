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
    _, key, *_ = row_tuple
    return key

def stream_feeder(stream_buffer, csv_path):
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

        time.sleep(0.001)  

if __name__=="__main__":
    
    DATA_PATH = '../../data/transactional_data.csv'
    df = pd.read_csv(DATA_PATH)

    stream_buffer = StreamBuffer()
    hash_table = HashTable()

    # Create threads
    feeder_thread = threading.Thread(
        target=stream_feeder, args=(stream_buffer, DATA_PATH), daemon=True
    )
    
    print(f"Slots Available: {hash_table.get_available_slots()}")

    # Main Outer Loop
    for idx in range(len(df)): 
        print(idx + 1)
        row_tuple: tuple = generate_tuple(df, idx)

        # Extracting Key (CustomerID) for Hashing
        key: int = extract_key(row_tuple)

        hash_table.insert(row_tuple, key)

        print(f"Slots Available: {hash_table.get_available_slots()}")
        
        
      

    