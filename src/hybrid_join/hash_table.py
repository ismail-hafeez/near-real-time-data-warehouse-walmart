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
        import os
        script_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_dir = os.path.join(script_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, 'Hashed_data.log')
        # Writing to log file
        with open(log_path, "a", encoding="utf-8") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"[{timestamp}] - {message}\n")

    def _hash(self, key):
        """Compute hash index for a key"""
        return hash(key) % self.hS

    def insert(self, key: int, value: tuple) -> None:
        """Inserts new entry (multi-map - allows multiple values per key)"""

        index = self._hash(key)
        # Append new key-value pair (multi-map behavior)
        self.table[index].append([key, value])
        self.slots_available -= 1
        #log_message: str = f"Inserted key {key} with value {value}"
        #self.log_hashed(log_message)

    def get_available_slots(self) -> int:
        """Returns number of available slots"""
        return max(0, self.slots_available)
    
    def get_total_entries(self) -> int:
        """Returns total number of entries in hash table"""
        total = 0
        for bucket in self.table:
            total += len(bucket)
        return total

    def get(self, key):
        """Get all values for a key (multi-map support)"""
        index = self._hash(key)
        results = []
        for pair in self.table[index]:
            if pair[0] == key:
                results.append(pair[1])
        return results if results else None  # Return list of all matches

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
