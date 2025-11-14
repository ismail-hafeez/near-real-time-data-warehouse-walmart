# Queue

# ETL

# Hybrid Join
# Key Components: hash table, queue, disk buffer, and a stream buffer.

"""
Queue: A doubly-linked list that stores the join attribute values (keys) from the stream tuples in FIFO order. 
Each node also has pointers to its neighbors. The queue tracks the order of arrival for fairness in processing.

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

if __name__=="__main__":
    
    DATA = '../../data/transactional_data.csv'
    df = pd.read_csv(DATA)

    # Main Outer Loop
    for idx in range(len(df)): 
        row_tuple = generate_tuple(df, idx)
        print(row_tuple)
        break


    