"""
Takes user and password for MySQL connector
Creates DW
"""

import sys
import mysql.connector
from datetime import datetime

class DWH:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password
        self.conn
        self.cur
        self.SQL_PATH = 'createDW.sql'
        self.POP_PATH = 'populateDW.sql'
        self.establish_connection()

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

    def populate_dw(self) -> None:
        """
        Populates DW with initial Data
        """
        log_mssg: str = ''
        try:
            # Read the SQL file
            with open(self.POP_PATH, 'r') as file:
                sql_script = file.read()

            # Split the script into individual statements
            statements = sql_script.split(';')

            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    self.cur.execute(stmt)

            self.conn.commit()
            log_mssg = "Data Warehouse populated successfully!"

        except Exception as e:
            self.conn.rollback()
            log_mssg = f"Error while populating DW: {e}"

        finally:
            self.cur.close()
            self.conn.close()
            self.log_db_donfig(log_mssg)

if __name__=="__main__":

    # Taking User input
    user: str = input("User (e.g root): ")
    password: str = input("Password: ")
    # DWH Object
    data_warehouse = DWH(user, password)
    # Creating DB
    data_warehouse.create_dw()  
    # Populate DW
    data_warehouse.populate_dw()  