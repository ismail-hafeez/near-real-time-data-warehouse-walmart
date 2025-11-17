"""
Queue: A doubly-linked list that stores the join attribute values (keys) from the stream tuples in FIFO order. 
Each node also has pointers to its neighbors. The queue tracks the order of arrival for fairness in processing.

"""

class Node:
    def __init__(self, key: int):
        self.key = key
        self.prev = None
        self.next = None

class Queue:
    def __init__(self):
        self.head = None
        self.tail = None

    def enqueue(self, key: int) -> None:
        node = Node(key)

        if self.tail is None:
            self.head = self.tail = node
        else:
            self.tail.next = node  
            node.prev = self.tail 
            self.tail = node

    def dequeue(self) -> int | None:
        if self.head is None:
            return None

        key = self.head.key
        self.head = self.head.next

        if self.head:
            self.head.prev = None
        else:
            self.tail = None  # queue is empty
            
        return key

    def is_empty(self) -> bool:
        return self.head is None
