"""
Hash Table (H): A multi-map (allows multiple entries per key) that stores stream tuples.
Each entry also includes a pointer (address) to a corresponding node in the queue. 
The hash table has a fixed number of slots hS = 10,000.
"""

from datetime import datetime

class HashTable:
    def __init__(self, hS=10000):
        self.hS = hS
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

    def insert(self, key, value) -> None:
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

    def delete(self, key) -> None:
        index = self._hash(key)
        for i, pair in enumerate(self.table[index]):
            if pair[0] == key:
                self.table[index].pop(i)
                log_message: str = f"Deleted key {key}"
                self.log_hashed(log_message)
                return
        log_message: str = f"Key {key} not found"
        self.log_hashed(log_message)

if __name__=="__main__":
    # Example usage
    ht = HashTable()

    ht.insert("apple", 10)
    ht.insert("banana", 20)
    ht.insert("orange", 30)

    print("Get apple:", ht.get("apple"))
    ht.delete("banana")
    print("Get banana:", ht.get("banana"))
