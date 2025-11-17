"""
Disk Buffer: A memory buffer that holds a loaded partition p of size vP from R. This is
part of the "Join Window." Each disk partition size will be 500 tuples.
"""

# disk_buffer.py

import pandas as pd

class DiskBuffer:
    def __init__(self, r_path, partition_size=500):
        self.df = pd.read_csv(r_path)
        self.df = self.df.sort_values("Customer_ID")
        self.partition_size = partition_size

    def load_partition(self, key: int) -> None:
        """
        Return a partition (slice of dataframe) centered on the Customer_ID.
        """
        # all tuples in R that match the key (Customer_ID)
        block = self.df[self.df["Customer_ID"] == key]

        if block.empty:
            return []

        # Take first match
        idx = block.index[0]

        start = max(0, idx)
        end   = min(len(self.df), start + self.partition_size)

        return self.df.iloc[start:end].to_dict("records")
