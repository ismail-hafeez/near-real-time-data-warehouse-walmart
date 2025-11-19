"""
Takes user and password for MySQL connector
Creates DW
"""

import sys
import mysql.connector
from datetime import datetime
import pandas as pd

class DWH:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password
        self.SQL_PATH = 'createDW.sql'
        self.POP_PATH = 'populateDW.sql'
        self.CUSTOMER_M_DATA = '../../data/customer_master_data.csv'
        self.PRODUCT_M_DATA = '../../data/product_master_data.csv'
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
    def log_db_donfig(message: str) -> None:
        PATH = "../../logs"
        # Writing to log file
        with open(f"{PATH}/create_DWH.log", "a", encoding="utf-8") as file:
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
            with open(self.SQL_PATH, 'r') as file:
                sql_script = file.read()

            # Split the script into individual statements
            statements = sql_script.split(';')

            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    self.cur.execute(stmt)

            self.conn.commit()
            log_mssg = "Data Warehouse created successfully from SQL script!"

        except Exception as e:
            self.conn.rollback()
            log_mssg = f"Error while executing SQL script: {e}"

        finally:
            self.cur.close()
            self.conn.close()
            self.log_db_donfig(log_mssg)

    # -- Main Functions -- #
    def populate_dim_customer(self) -> None:
        
        customer_df = pd.read_csv(self.CUSTOMER_M_DATA)

        for index in range(len(customer_df)):
            row_tup = self.generate_tuple_customer(customer_df, index)

            try:
                query = """
                    INSERT INTO walmart_dw.dimcustomer (
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
                self.conn.commit()
                print("Dim_Customer table successfully populated")        

            except Exception as e:
                print(f"Error Populating DW: {e}")        

    def populate_dim_product(self) -> None:
        
        product_df = pd.read_csv(self.PRODUCT_M_DATA)

        for index in range(len(product_df)):
            row_tup = self.generate_tuple_product(product_df, index)

            try:
                query = """
                    INSERT INTO walmart_dw.dimproduct (
                        Product_ID,
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
                self.conn.commit()
                print("Dim_Customer table successfully populated")        

            except Exception as e:
                print(f"Error Populating DW: {e}")    

if __name__=="__main__":

    # Taking User input
    user: str = input("User (e.g root): ")
    password: str = input("Password: ")
    # DWH Object
    data_warehouse = DWH(user, password)
    # Creating DB
    print('Creating Data Warehouse')
    data_warehouse.create_dw()  
    # Populate DW
    print('Populating Data Warehouse')
    data_warehouse.populate_dim_customer()  
    
    print('Success')