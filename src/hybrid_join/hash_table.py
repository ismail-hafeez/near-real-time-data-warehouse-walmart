"""
Hash Table (H): A multi-map (allows multiple entries per key) that stores stream tuples.
Each entry also includes a pointer (address) to a corresponding node in the queue. 
The hash table has a fixed number of slots hS = 10,000.
"""

from datetime import datetime

class HashTable:
    def __init__(self, hS=10000):
        self.hS = 10000
        self.slots_available = hS
        self.table = [[] for _ in range(hS)]  # list of lists for chaining

    @staticmethod
    def log_hashed(message: str) -> None:
        PATH = "../../logs"
        # Writing to log file
        with open(f"{PATH}/Hashed_data.log", "a", encoding="utf-8") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"[{timestamp}] - {message}\n")

    def _hash(self, key):
        """Compute hash index for a key"""
        return hash(key) % self.hS

    def insert(self, key: int, value: tuple) -> None:
        """Inserts new entry or updates existing"""

        index = self._hash(key)
        # Check if key exists and update
        for pair in self.table[index]:
            if pair[0] == key:
                pair[1] = value
                log_message: str = f"Updated key {key} with value {value}"
                self.log_hashed(log_message)
                return
        # Key not found, append new key-value pair
        self.table[index].append([key, value])
        self.slots_available -= 1
        log_message: str = f"Inserted key {key} with value {value}"
        self.log_hashed(log_message)

    def get_available_slots(self) -> int:
        return self.slots_available

    def get(self, key):
        index = self._hash(key)
        for pair in self.table[index]:
            if pair[0] == key:
                return pair[1]
        return None  # Key not found

    def delete(self, key: int, value: tuple) -> bool:
        index = self._hash(key)
        bucket = self.table[index]

        for i, pair in enumerate(bucket):
            k, v = pair

            if k == key and v == value:
                bucket.pop(i)
                self.slots_available += 1
                return True

        return False

if __name__=="__main__":
    ...
