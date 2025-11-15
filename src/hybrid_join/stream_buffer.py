"""
Stream Buffer: A small buffer to temporarily hold incoming stream tuples if the algorithm
can't process them immediately. This prevents loss of data in bursty scenarios.
"""

from collections import deque
from datetime import datetime

class StreamBuffer:
    def __init__(self):
        self.buffer = deque()  

    @staticmethod
    def log_stream(message: str) -> None:
        PATH = "../../logs"
        # Writing to log file
        with open(f"{PATH}/Stream_buffer.log", "a", encoding="utf-8") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"[{timestamp}] - {message}\n")
        
    def push(self, data: tuple) -> None:
        self.buffer.append(data)
        log_message: str = f"Added: {data}, Buffer: {list(self.buffer)}"
        self.log_stream(log_message)

    def get(self):
        if self.buffer:
            item = self.buffer.popleft()
            print(f"Retrieved: {item}, Buffer: {list(self.buffer)}")
            return item
        else:
            print("Buffer empty!")
            return None
        
    def size(self) -> int:
        return len(self.buffer)   

if __name__=="__main__":
    ...