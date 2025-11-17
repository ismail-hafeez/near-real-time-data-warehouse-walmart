"""
Stream Buffer: A small buffer to temporarily hold incoming stream tuples if the algorithm
can't process them immediately. This prevents loss of data in bursty scenarios.
"""

from collections import deque
from datetime import datetime
import threading

class StreamBuffer:
    def __init__(self):
        self.buffer = deque()  
        self.lock = threading.Lock()

    @staticmethod
    def log_stream(message: str) -> None:
        PATH = "../../logs"
        # Writing to log file
        with open(f"{PATH}/Stream_buffer.log", "a", encoding="utf-8") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"[{timestamp}] - {message}\n")
        
    def push(self, data: tuple) -> None:
        with self.lock:
            self.buffer.append(data)
        log_message: str = f"Added: {data}, Buffer: {list(self.buffer)}"
        self.log_stream(log_message)

    def pop(self) -> tuple | None:
        with self.lock:
            if self.buffer:
                item = self.buffer.popleft()
                log_message: str = f"Retrieved: {item}, Buffer: {list(self.buffer)}"
                self.log_stream(log_message)
                return item
            else:
                log_message: str = "Buffer empty!"
                self.log_stream(log_message)
                return None 
        
    def size(self) -> int:
        with self.lock:
            return len(self.buffer)   

    def is_empty(self):
        with self.lock:
            return len(self.buffer) == 0

if __name__=="__main__":
    ...