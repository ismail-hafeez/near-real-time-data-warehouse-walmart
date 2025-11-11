# Queue

# ETL


# Hybrid Join
# Key Components: hash table, queue, disk buffer, and a stream buffer.

"""
Stream Buffer: A small buffer to temporarily hold incoming stream tuples if the algorithm
can't process them immediately. This prevents loss of data in bursty scenarios.

Hash Table (H): A multi-map (allows multiple entries per key) that stores stream tuples.
Each entry also includes a pointer (address) to a corresponding node in the queue. The
hash table has a fixed number of slots hS = 10,000.

Queue: A doubly-linked list that stores the join attribute values (keys) from the stream tuples in FIFO order. 
Each node also has pointers to its neighbors. The queue tracks the order of arrival for fairness in processing.

Disk Buffer: A memory buffer that holds a loaded partition p of size vP from R. This is
part of the "Join Window." Each disk partition size will be 500 tuples.

Stream Input (w): The number of available slots in the hash table which are equal to the
number of free up spaces in the previous iteration.

import pandas as pd

df = pd.read_csv('../../data/transactional_data.csv')

for i in range(len(df)):
    row = df.iloc[i]  # or df.loc[i] for label-based indexing
    print(row['column_name'])  # replace 'column_name' with the actual column name

"""
import pandas as pd

if __name__=="__main__":
    DATA = '../../data/transactional_data.csv'

    df = pd.read_csv(DATA)

    row = df.iloc[0]
    print(row)

    