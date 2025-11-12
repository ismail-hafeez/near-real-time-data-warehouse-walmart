"""
Hash Table (H): A multi-map (allows multiple entries per key) that stores stream tuples.
Each entry also includes a pointer (address) to a corresponding node in the queue. 
The hash table has a fixed number of slots hS = 10,000.
"""

class HashTable:
    def __init__(self, hS=10000):
        self.hS = hS
        self.slots_available = hS
        self.table = [[] for _ in range(hS)]  # list of lists for chaining

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
                print(f"Updated key {key} with value {value}")
                return
        # Key not found, append new key-value pair
        self.table[index].append([key, value])
        self.slots_available -= 1
        print(f"Inserted key {key} with value {value}")

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
                print(f"Deleted key {key}")
                return
        print(f"Key {key} not found")

# Example usage
ht = HashTable()

ht.insert("apple", 10)
ht.insert("banana", 20)
ht.insert("orange", 30)

print("Get apple:", ht.get("apple"))
ht.delete("banana")
print("Get banana:", ht.get("banana"))
