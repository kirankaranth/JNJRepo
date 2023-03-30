"""
This module performs DB connectivity and provides functionality to process DB queries
"""
from __future__ import print_function
import traceback
from DatabaseLibrary import DatabaseLibrary


class AzureDBLib(DatabaseLibrary):
    "Handles Databaselibrary through overriding and extending functionality."
    ROBOT_LIBRARY_SCOPE = "TEST SUITE"

    def __init__(self):
        self.connected = False
        self.orm_con = None
        self.column_names = []

    def connect_to_azuredb(self, db_conn_string, database, uid, password, port):
        """
        Connects to Azure Database.

        :param db_conn_string: Database connection string as inferred by Azure portal
        :param database: Database to connect to in Azure environment
        :param uid: Username to connect to Azure DB
        :param password: Password to connect to Azure DB
        :param port: Port Number of the Azure DB
        """
        try:
            driver_string = self._get_driver_string()
            driver = "'DRIVER={%s};" % driver_string
            server = "SERVER=" + db_conn_string + ";"
            port = "PORT=" + str(port) + ";"
            database = "DATABASE=" + database + ";"
            uid = "UID=" + uid + ";"
            pwd = "PWD=" + password + "'"

            conn_string = driver + server + port + database + uid + pwd

            self.connect_to_database_using_custom_params(
                dbapiModuleName="pyodbc", db_connect_string=conn_string
            )
            self.connected = True
        except:
            print(traceback.format_exc())
            print("Failed to connect to Azure DB")

    def query_using_orm(self, query):
        """
        Executes the query using the orm
        :param query: String of sql query to execute
        :return: Result from running the query
        """
        result = []
        if self.orm_con:
            result_set = self.orm_con.execute(query)
            self.column_names = result_set.keys()
            result = result_set.fetchall()
        return result

    def disconnect_from_azuredb(self):
        "Disconnects from Azure Database"
        try:
            if self.connected:
                self.disconnect_from_database()
                self.connected = False
        except:
            print(traceback.format_exc())
            print("Failed to disconnect from Azure DB")
