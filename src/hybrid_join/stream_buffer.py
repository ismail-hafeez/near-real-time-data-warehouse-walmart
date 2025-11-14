"""
Stream Buffer: A small buffer to temporarily hold incoming stream tuples if the algorithm
can't process them immediately. This prevents loss of data in bursty scenarios.
"""

from collections import deque

class StreamBuffer:
    def __init__(self, max_size):
        self.buffer = deque()  # double-ended queue
        self.max_size = max_size

    def add(self, item):
        if len(self.buffer) >= self.max_size:
            print("Buffer full! Removing oldest item.")
            self.buffer.popleft()  # remove oldest element
        self.buffer.append(item)
        print(f"Added: {item}, Buffer: {list(self.buffer)}")

    def get(self):
        if self.buffer:
            item = self.buffer.popleft()
            print(f"Retrieved: {item}, Buffer: {list(self.buffer)}")
            return item
        else:
            print("Buffer empty!")
            return None
        
if __name__=="__main__":
    # Example usage
    buffer = StreamBuffer(max_size=3)
    buffer.add(1)
    buffer.add(2)
    buffer.add(3)
    buffer.add(4)  # Buffer full, 1 will be removed
    buffer.get()
    buffer.get()
    buffer.get()
    buffer.get()  # Buffer empty
