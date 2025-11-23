"""
Takes user and password for MySQL connector
Creates DW
"""

import sys
import os
import mysql.connector
from datetime import datetime
import pandas as pd
import random

class DWH:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.SQL_PATH = os.path.join(script_dir, 'createDW.sql')
        self.CUSTOMER_M_DATA = os.path.join(script_dir, '../../data/customer_master_data.csv')
        self.PRODUCT_M_DATA = os.path.join(script_dir, '../../data/product_master_data.csv')
        self.establish_connection()

    # -- Helper Functions -- #
    @staticmethod
    def generate_tuple_customer(df: pd.DataFrame, index: int) -> tuple:

        row = df.iloc[index]

        Customer_ID = int(row.Customer_ID)
        gender = str(row.Gender)
        age = str(row.Age)
        occupation = int(row.Occupation)
        City_Category = str(row.City_Category)
        yrs = str(row.Stay_In_Current_City_Years)
        Marital_Status = int(row.Marital_Status)

        row_tuple = (Customer_ID, gender, age, occupation, City_Category, yrs, Marital_Status)

        return row_tuple

    @staticmethod
    def generate_tuple_product(df: pd.DataFrame, index: int) -> tuple:

        row = df.iloc[index]

        Product_ID = str(row.Product_ID)
        Product_Category = str(row.Product_Category) if pd.notna(row.Product_Category) else None
        Supplier_ID = int(row.supplierID) if pd.notna(row.supplierID) else None
        Supplier_Name = str(row.supplierName) if pd.notna(row.supplierName) else None
        
        # Generate random product name based on category
        Product_Name = DWH.generate_product_name(Product_Category, Product_ID)

        row_tuple = (Product_ID, Product_Category, Product_Name, Supplier_ID, Supplier_Name)

        return row_tuple
    
    @staticmethod
    def generate_product_name(category: str, product_id: str) -> str:
        """Generate a random product name based on category and product ID"""
        if category is None:
            category = "Product"
        
        # Product name prefixes/suffixes based on category
        category_prefixes = {
            "Home & Kitchen": ["Premium", "Deluxe", "Classic", "Modern", "Elegant", "Professional"],
            "Grocery": ["Fresh", "Organic", "Premium", "Natural", "Farm", "Garden"],
            "Pets": ["Pet", "Furry", "Comfort", "Premium", "Natural", "Healthy"],
            "Electronics": ["Smart", "Digital", "Pro", "Ultra", "Advanced", "Tech"],
            "Clothing": ["Fashion", "Style", "Comfort", "Premium", "Designer", "Classic"],
            "Sports": ["Pro", "Athletic", "Performance", "Elite", "Active", "Sport"],
            "Books": ["Classic", "Best", "Essential", "Complete", "Guide", "Collection"]
        }
        
        # Generic prefixes if category not found
        prefixes = category_prefixes.get(category, ["Premium", "Quality", "Standard", "Classic", "Modern"])
        
        # Product type suffixes
        suffixes = ["Item", "Product", "Goods", "Merchandise", "Article", "Unit"]
        
        # Use last 4 digits of Product_ID for uniqueness
        id_suffix = product_id[-4:] if len(product_id) >= 4 else product_id
        
        # Generate name
        prefix = random.choice(prefixes)
        suffix = random.choice(suffixes)
        product_name = f"{prefix} {category} {suffix} {id_suffix}"
        
        return product_name
    
    @staticmethod
    def log_db_donfig(message: str) -> None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, '../../logs')
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, 'create_DWH.log')
        # Writing to log file
        with open(log_path, "a", encoding="utf-8") as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"[{timestamp}] - {message}\n")
    
    def establish_connection(self) -> None:
        """
        Connects with MySQL
        Exits if failure
        """
        log_mssg: str = ''
        try:
            # Create conn/cur 
            self.conn = mysql.connector.connect(
                host="localhost",
                user=self.user,
                password=self.password
            )   
            self.cur = self.conn.cursor()
            print("Successfully Connected to MySQL")
            log_mssg = f"Successfully Connected to MySQL (USER: {self.user}, PIN: {self.password})"
            self.log_db_donfig(log_mssg)

        except Exception as e:
            log_mssg = f"Failed to Connect to MySQL -> {e}"
            self.log_db_donfig(log_mssg)
            sys.exit(f"Program terminated: Failed to Connect to MySQL -> {e}")
    
    def create_dw(self) -> None:
        """
        Executes SQL script to create DWH
        """
        log_mssg: str = ''
        try:
            # Read the SQL file
            with open(self.SQL_PATH, 'r', encoding='utf-8') as file:
                sql_script = file.read()

            # Split the script into individual statements
            statements = sql_script.split(';')

            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    self.cur.execute(stmt)

            self.conn.commit()
            log_mssg = "Data Warehouse created successfully from SQL script!"
            print(log_mssg)

        except Exception as e:
            self.conn.rollback()
            log_mssg = f"Error while executing SQL script: {e}"
            print(log_mssg)
            raise

        self.log_db_donfig(log_mssg)

    # -- Main Functions -- #
    def populate_dim_customer(self) -> None:
        """Populate DimCustomer dimension table"""
        if not self.conn.is_connected():
            self.establish_connection()
        
        customer_df = pd.read_csv(self.CUSTOMER_M_DATA)
        inserted = 0

        for index in range(len(customer_df)):
            row_tup = self.generate_tuple_customer(customer_df, index)

            try:
                query = """
                    INSERT INTO walmart_dw.DimCustomer (
                        Customer_ID,
                        Gender,
                        Age,
                        Occupation,
                        City_Category,
                        Stay_In_Current_City_Years,
                        Marital_Status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                self.cur.execute(query, row_tup)
                inserted += 1
                if inserted % 100 == 0:
                    print(f"Inserted {inserted} customer records...")

            except Exception as e:
                print(f"Error inserting customer {row_tup[0]}: {e}")
        
        self.conn.commit()
        print(f"DimCustomer table successfully populated with {inserted} records")

    def populate_dim_product(self) -> None:
        """Populate DimProduct dimension table"""
        if not self.conn.is_connected():
            self.establish_connection()
        
        product_df = pd.read_csv(self.PRODUCT_M_DATA)
        inserted = 0

        for index in range(len(product_df)):
            row_tup = self.generate_tuple_product(product_df, index)

            try:
                query = """
                    INSERT INTO walmart_dw.DimProduct (
                        Product_ID,
                        Product_Category,
                        Product_Name,
                        Supplier_ID,
                        Supplier_Name
                    )
                    VALUES (%s, %s, %s, %s, %s);
                """
                self.cur.execute(query, row_tup)
                inserted += 1
                if inserted % 100 == 0:
                    print(f"Inserted {inserted} product records...")

            except Exception as e:
                print(f"Error inserting product {row_tup[0]}: {e}")
        
        self.conn.commit()
        print(f"DimProduct table successfully populated with {inserted} records")

    def populate_dim_store(self) -> None:
        """Populate DimStore dimension table from product master data"""
        if not self.conn.is_connected():
            self.establish_connection()
        
        product_df = pd.read_csv(self.PRODUCT_M_DATA)
        # Extract unique stores
        stores = product_df[['storeID', 'storeName']].drop_duplicates()
        inserted = 0

        for _, row in stores.iterrows():
            try:
                store_id = int(row.storeID)
                store_name = str(row.storeName)
                # Extract city category from store name or use default
                city_category = 'A'  # Default, can be enhanced
                region = 'Unknown'  # Default, can be enhanced

                query = """
                    INSERT IGNORE INTO walmart_dw.DimStore (
                        Store_ID,
                        Store_Name,
                        Store_City_Category,
                        Store_Region
                    )
                    VALUES (%s, %s, %s, %s);
                """
                self.cur.execute(query, (store_id, store_name, city_category, region))
                inserted += 1

            except Exception as e:
                print(f"Error inserting store {row.storeID}: {e}")
        
        self.conn.commit()
        print(f"DimStore table successfully populated with {inserted} records")

    def populate_dim_supplier(self) -> None:
        """Populate DimSupplier dimension table from product master data"""
        if not self.conn.is_connected():
            self.establish_connection()
        
        product_df = pd.read_csv(self.PRODUCT_M_DATA)
        # Extract unique suppliers
        suppliers = product_df[['supplierID', 'supplierName']].drop_duplicates()
        inserted = 0

        for _, row in suppliers.iterrows():
            try:
                supplier_id = int(row.supplierID)
                supplier_name = str(row.supplierName)

                query = """
                    INSERT IGNORE INTO walmart_dw.DimSupplier (
                        Supplier_ID,
                        Supplier_Name
                    )
                    VALUES (%s, %s);
                """
                self.cur.execute(query, (supplier_id, supplier_name))
                inserted += 1

            except Exception as e:
                print(f"Error inserting supplier {row.supplierID}: {e}")
        
        self.conn.commit()
        print(f"DimSupplier table successfully populated with {inserted} records")

    def populate_dim_date(self) -> None:
        """Populate DimDate dimension table from transactional data"""
        if not self.conn.is_connected():
            self.establish_connection()
        
        trans_df = pd.read_csv(os.path.join(os.path.dirname(self.CUSTOMER_M_DATA), 'transactional_data.csv'))
        # Extract unique dates
        trans_df['date'] = pd.to_datetime(trans_df['date'])
        unique_dates = trans_df['date'].drop_duplicates().sort_values()
        inserted = 0

        for date_val in unique_dates:
            try:
                date_id = int(date_val.strftime('%Y%m%d'))
                day = date_val.day
                month = date_val.month
                month_name = date_val.strftime('%B')
                quarter = (month - 1) // 3 + 1
                year = date_val.year
                day_of_week = date_val.strftime('%A')
                is_weekend = 1 if day_of_week in ['Saturday', 'Sunday'] else 0
                
                # Determine season
                if month in [12, 1, 2]:
                    season = 'Winter'
                elif month in [3, 4, 5]:
                    season = 'Spring'
                elif month in [6, 7, 8]:
                    season = 'Summer'
                else:
                    season = 'Fall'
                
                week_of_year = date_val.isocalendar()[1]

                query = """
                    INSERT IGNORE INTO walmart_dw.DimDate (
                        Date_ID,
                        Full_Date,
                        Day,
                        Month,
                        Month_Name,
                        Quarter,
                        Year,
                        Day_Of_Week,
                        Is_Weekend,
                        Season,
                        Week_Of_Year
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                self.cur.execute(query, (date_id, date_val.date(), day, month, month_name, 
                                       quarter, year, day_of_week, is_weekend, season, week_of_year))
                inserted += 1
                if inserted % 100 == 0:
                    print(f"Inserted {inserted} date records...")

            except Exception as e:
                print(f"Error inserting date {date_val}: {e}")
        
        self.conn.commit()
        print(f"DimDate table successfully populated with {inserted} records")

    def close_connection(self) -> None:
        """Close database connection"""
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.conn.close()
            print("Database connection closed")    

if __name__=="__main__":

    # Taking User input
    user: str = input("User (e.g root): ")
    password: str = input("Password: ")
    # DWH Object
    data_warehouse = DWH(user, password)
    # Creating DB
    print('Creating Data Warehouse')
    data_warehouse.create_dw()
    # Re-establish connection after create_dw closes it
    data_warehouse.establish_connection()
    
    # Populate DW
    print('Populating Data Warehouse')
    print('Populating DimCustomer...')
    data_warehouse.populate_dim_customer()
    print('Populating DimProduct...')
    data_warehouse.populate_dim_product()
    print('Populating DimStore...')
    data_warehouse.populate_dim_store()
    print('Populating DimSupplier...')
    data_warehouse.populate_dim_supplier()
    print('Populating DimDate...')
    data_warehouse.populate_dim_date()
    
    # Close connection
    data_warehouse.close_connection()
    print('Success - Data Warehouse populated!')