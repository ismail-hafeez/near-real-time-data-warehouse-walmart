"""
Takes user and password for MySQL connector
Creates DW
"""

import sys

class DWH:
    def __init__(self, user: str, password: str) -> None:
        self.user = user
        self.password = password

    def establish_connection(self) -> bool:
        """
        Connects with MySQL
        Returns 1 if success
        Return 0 if failure
        """
        return False
    
    def connection_success(self) -> bool:

        isSuccess: bool = data_warehouse.establish_connection()
        if not isSuccess:
            sys.exit("Program terminated: Failed to Connect to MySQL")
        else:
            # Create conn/cur    
            print("Successfully Connected to MySQL")
            return True
    
    def create_dw(self):
        ...

if __name__=="__main__":

    # Taking User input
    user: str = input("User: ")
    password: str = input("Password: ")
    # DWH Object
    data_warehouse = DWH(user, password)
    data_warehouse.connection_success()

    print("hello")
    