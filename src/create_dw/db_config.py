"""
Takes user and password for MySQL connector
Creates DW
"""

import sys
import mysql.connector

class DWH:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None
        self.establish_connection()

    def establish_connection(self) -> None:
        """
        Connects with MySQL
        Exits if failure
        """
        try:
            # Create conn/cur 
            self.conn = mysql.connector.connect(
                host="localhost",
                user=self.user,
                password=self.password
            )   
            cur = self.conn.cursor()
            print("Successfully Connected to MySQL")
        except Exception as e:
            sys.exit(f"Program terminated: Failed to Connect to MySQL -> {e}")
    
    def create_dw(self):
        ...

if __name__=="__main__":

    # Taking User input
    user: str = input("User: ")
    password: str = input("Password: ")
    # DWH Object
    data_warehouse = DWH(user, password)

    