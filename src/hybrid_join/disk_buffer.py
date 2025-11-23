"""
Disk Buffer: A memory buffer that holds a loaded partition p of size vP from R. This is
part of the "Join Window." Each disk partition size will be 500 tuples.
"""

import pandas as pd

class DiskBuffer:
    def __init__(self, r_path, partition_size=500, key_column=None):
        self.df = pd.read_csv(r_path)
        self.partition_size = partition_size
        
        # Determine key column automatically or use provided
        if key_column:
            self.key_column = key_column
        elif "Customer_ID" in self.df.columns:
            self.key_column = "Customer_ID"
            self.df = self.df.sort_values("Customer_ID")
        elif "Product_ID" in self.df.columns:
            self.key_column = "Product_ID"
            self.df = self.df.sort_values("Product_ID")
        else:
            # Use first column as key
            self.key_column = self.df.columns[0]
            self.df = self.df.sort_values(self.key_column)

    def load_partition(self, key) -> list:
        """
        Return a partition (slice of dataframe) centered on the key.
        For Customer_ID: returns matching customer records
        For Product_ID: returns matching product records
        """
        # Check if key is int or str based on column type
        if self.key_column == "Customer_ID":
            key = int(key)
        elif self.key_column == "Product_ID":
            key = str(key)
        
        # All tuples in R that match the key
        block = self.df[self.df[self.key_column] == key]

        if block.empty:
            return []

        # For exact matches, return all matching records
        # For partition-based approach, return surrounding records
        if len(block) <= self.partition_size:
            # Return all matches plus surrounding context
            first_idx = block.index[0]
            start = max(0, first_idx - self.partition_size // 2)
            end = min(len(self.df), start + self.partition_size)
            return self.df.iloc[start:end].to_dict("records")
        else:
            # Too many matches, return first partition_size
            return block.iloc[:self.partition_size].to_dict("records")
