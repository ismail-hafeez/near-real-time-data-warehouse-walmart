"""
HYBRIDJOIN Algorithm Implementation for Near-Real-Time Data Warehouse

The stream buffer will continuously get transactional data as the HYBRIDJOIN is
for the near real time data. A thread will be implemented which will continuously get data
from the transactional_data.csv provided into the stream buffer independent of the join
operation. Then there will be another thread which will implement the HYBRIDJOIN
algorithm.
"""

import os
import sys
import pandas as pd
import mysql.connector
from datetime import datetime
from stream_buffer import StreamBuffer
from hash_table import HashTable
from disk_buffer import DiskBuffer
from queue import Queue
import threading
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class HybridJoinETL:
    def __init__(self, db_user: str, db_password: str, 
                 transaction_csv: str, customer_master_csv: str, product_master_csv: str):
        self.db_user = db_user
        self.db_password = db_password
        self.transaction_csv = transaction_csv
        self.customer_master_csv = customer_master_csv
        self.product_master_csv = product_master_csv
        
        # Initialize data structures
        self.stream_buffer = StreamBuffer()
        self.hash_table = HashTable()
        self.queue = Queue()
        self.customer_disk_buffer = DiskBuffer(customer_master_csv, partition_size=500, key_column="Customer_ID")
        self.product_disk_buffer = DiskBuffer(product_master_csv, partition_size=500, key_column="Product_ID")
        
        # Database connection (will be established in worker thread)
        self.conn = None
        self.cur = None
        
        # Load product master data into memory for quick lookup
        self.product_df = pd.read_csv(product_master_csv)
        self.product_lookup = {}
        for _, row in self.product_df.iterrows():
            self.product_lookup[str(row['Product_ID'])] = {
                'storeID': int(row['storeID']),
                'price': float(row['price$'])
            }
        
        # Statistics
        self.processed_count = 0
        self.loaded_count = 0
        self.lock = threading.Lock()
        
    def establish_db_connection(self):
        """Establish database connection"""
        try:
            self.conn = mysql.connector.connect(
                host="localhost",
                user=self.db_user,
                password=self.db_password,
                database="walmart_dw"
            )
            self.cur = self.conn.cursor()
            print("Database connection established")
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            raise
    
    def get_date_id(self, date_str: str) -> int:
        """Convert date string to Date_ID format (YYYYMMDD)"""
        try:
            date_obj = pd.to_datetime(date_str)
            return int(date_obj.strftime('%Y%m%d'))
        except:
            return None
    
    def get_store_id(self, product_id: str) -> int:
        """Get Store_ID from product lookup"""
        if product_id in self.product_lookup:
            return self.product_lookup[product_id]['storeID']
        return None
    
    def load_to_dw(self, enriched_tuple: dict):
        """Load enriched transaction into FactSales table"""
        try:
            # Get Date_ID
            date_id = self.get_date_id(enriched_tuple['date'])
            if date_id is None:
                return False
            
            # Get Store_ID
            store_id = self.get_store_id(enriched_tuple['Product_ID'])
            if store_id is None:
                return False
            
            # Calculate purchase amount
            purchase_amount = enriched_tuple.get('purchase_amount', 0)
            
            query = """
                INSERT INTO walmart_dw.FactSales (
                    Order_ID,
                    Customer_ID,
                    Product_ID,
                    Date_ID,
                    Store_ID,
                    Purchase_Amount,
                    Quantity
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                enriched_tuple['orderID'],
                enriched_tuple['Customer_ID'],
                enriched_tuple['Product_ID'],
                date_id,
                store_id,
                purchase_amount,
                enriched_tuple['quantity']
            )
            
            self.cur.execute(query, values)
            self.conn.commit()
            
            with self.lock:
                self.loaded_count += 1
                if self.loaded_count % 100 == 0:
                    print(f"Loaded {self.loaded_count} records into DW...")
            
            return True
            
        except Exception as e:
            print(f"Error loading to DW: {e}")
            self.conn.rollback()
            return False

def generate_tuple(df: pd.DataFrame, index: int) -> tuple:
    """Return a tuple for each transactional row"""
    row = df.iloc[index]
    orderID = int(row.orderID)
    Customer_ID = int(row.Customer_ID)
    Product_ID = str(row.Product_ID)
    quantity = int(row.quantity)
    date = str(row.date)
    trans_tuple = (orderID, Customer_ID, Product_ID, quantity, date)
    return trans_tuple

def extract_key(tup: tuple) -> int:
    """Extracting Key (CustomerID) for Hashing"""
    _, key, *_ = tup
    return key

def stream_feeder(stream_buffer: StreamBuffer, csv_path: str, stop_event: threading.Event) -> None:
    """
    Continuously read the CSV and push tuples into stream buffer.
    Simulates a real-time transactional stream.
    """
    df = pd.read_csv(csv_path)
    idx = 0
    
    while not stop_event.is_set() and idx < len(df):
        row_tuple: tuple = generate_tuple(df, idx)
        stream_buffer.push(row_tuple)
        idx += 1
        time.sleep(0.0001)  # Simulate streaming delay
    
    print(f"Stream feeder finished. Processed {idx} transactions.")

def hybridjoin_worker(etl: HybridJoinETL, stop_event: threading.Event) -> None:
    """
    Continuously runs the Hybrid Join algorithm.
    Implements the HYBRIDJOIN algorithm as described in the project.
    """
    # Establish database connection in worker thread
    etl.establish_db_connection()
    
    stream_buffer = etl.stream_buffer
    hash_table = etl.hash_table
    queue = etl.queue
    customer_disk_buffer = etl.customer_disk_buffer
    product_disk_buffer = etl.product_disk_buffer
    
    print("HYBRIDJOIN worker started")
    
    while not stop_event.is_set():
        # Step 1: Check available hash table slots (w)
        slots_available = hash_table.get_available_slots()
        
        # Step 2: Load up to w stream tuples into hash table
        loaded = 0
        while loaded < slots_available and not stream_buffer.is_empty():
            row: tuple = stream_buffer.pop()
            if row is None:
                break
            
            key = extract_key(row)  # Customer_ID
            hash_table.insert(key, row)
            queue.enqueue(key)
            loaded += 1
            
            with etl.lock:
                etl.processed_count += 1
        
        # Step 3: Get oldest key from queue
        oldest_key = queue.dequeue()
        if oldest_key is None:
            # No data to process, wait a bit
            time.sleep(0.01)
            continue
        
        # Step 4: Load disk partition for customer master data
        customer_partition = customer_disk_buffer.load_partition(oldest_key)
        if not customer_partition:
            # No customer data found, skip
            continue
        
        # Step 5: Probe hash table with customer partition
        stream_matches = hash_table.get(oldest_key)
        if not stream_matches:
            # No matches for this key, continue to next key
            continue
        
        # Step 6: Join stream tuples with customer master data
        for stream_tuple in stream_matches:
            orderID, Customer_ID, Product_ID, quantity, date = stream_tuple
            
            # Find matching customer record
            customer_record = None
            for cust_record in customer_partition:
                if cust_record.get('Customer_ID') == Customer_ID:
                    customer_record = cust_record
                    break
            
            if customer_record is None:
                continue
            
            # Step 7: Load product master data partition
            product_partition = product_disk_buffer.load_partition(Product_ID)
            product_record = None
            
            if product_partition:
                # Find exact match
                for prod in product_partition:
                    if str(prod.get('Product_ID', '')) == Product_ID:
                        product_record = prod
                        break
                
                if product_record:
                    purchase_amount = float(product_record.get('price$', 0)) * quantity
                else:
                    # Try lookup
                    if Product_ID in etl.product_lookup:
                        product_info = etl.product_lookup[Product_ID]
                        purchase_amount = product_info['price'] * quantity
                    else:
                        continue
            else:
                # Try to get product info from lookup
                if Product_ID in etl.product_lookup:
                    product_info = etl.product_lookup[Product_ID]
                    purchase_amount = product_info['price'] * quantity
                else:
                    continue
            
            # Step 8: Create enriched tuple
            enriched_tuple = {
                'orderID': orderID,
                'Customer_ID': Customer_ID,
                'Product_ID': Product_ID,
                'quantity': quantity,
                'date': date,
                'purchase_amount': purchase_amount,
                'customer_data': customer_record,
                'product_data': product_record if product_partition else None
            }
            
            # Step 9: Load enriched data into DW
            success = etl.load_to_dw(enriched_tuple)
            
            if success:
                # Step 10: Remove matched tuple from hash table
                hash_table.delete(oldest_key, stream_tuple)
    
    # Close database connection
    if etl.cur:
        etl.cur.close()
    if etl.conn and etl.conn.is_connected():
        etl.conn.close()
    
    print(f"HYBRIDJOIN worker finished. Processed {etl.processed_count} transactions, loaded {etl.loaded_count} records.")

if __name__ == "__main__":
    # Get database credentials
    print("HYBRIDJOIN ETL System")
    print("=" * 50)
    db_user = input("MySQL User (e.g., root): ")
    db_password = input("MySQL Password: ")
    
    # File paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    TRANSACTION_CSV = os.path.join(script_dir, '../../data/transactional_data.csv')
    CUSTOMER_MASTER_CSV = os.path.join(script_dir, '../../data/customer_master_data.csv')
    PRODUCT_MASTER_CSV = os.path.join(script_dir, '../../data/product_master_data.csv')
    
    # Initialize ETL system
    etl = HybridJoinETL(
        db_user=db_user,
        db_password=db_password,
        transaction_csv=TRANSACTION_CSV,
        customer_master_csv=CUSTOMER_MASTER_CSV,
        product_master_csv=PRODUCT_MASTER_CSV
    )
    
    # Create stop event for graceful shutdown
    stop_event = threading.Event()
    
    # Create threads
    feeder_thread = threading.Thread(
        target=stream_feeder,
        args=(etl.stream_buffer, TRANSACTION_CSV, stop_event),
        daemon=True
    )
    join_thread = threading.Thread(
        target=hybridjoin_worker,
        args=(etl, stop_event),
        daemon=True
    )
    
    # Start threads
    print("\nStarting ETL process...")
    feeder_thread.start()
    join_thread.start()
    
    try:
        # Keep main thread alive and monitor progress
        while True:
            time.sleep(3)
            with etl.lock:
                print(f"Progress: Processed {etl.processed_count} transactions, Loaded {etl.loaded_count} records into DW")
            
            # Check if threads are still alive
            if not feeder_thread.is_alive() and not join_thread.is_alive():
                break
                
    except KeyboardInterrupt:
        print("\nStopping ETL process...")
        stop_event.set()
        time.sleep(2)
    
    print(f"\nETL Complete!")
    print(f"Total processed: {etl.processed_count}")
    print(f"Total loaded to DW: {etl.loaded_count}")
