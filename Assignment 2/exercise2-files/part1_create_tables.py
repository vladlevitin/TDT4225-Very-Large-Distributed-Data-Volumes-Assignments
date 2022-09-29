from DbConnector import DbConnector
from tabulate import tabulate
import pandas as pd
import sqlalchemy
from sqlalchemy.pool import StaticPool
import glob
import os
import multiprocessing



class Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name, query = None):
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_data(self, table_name):
        
        self.db_connection.commit()

    def fetch_data(self, table_name, limit = None):
        
        if limit:
            query = "SELECT * FROM %s"
            self.cursor.execute(query % table_name + " LIMIT " + str(limit))
            rows = self.cursor.fetchall()
            print("Data from table %s, raw format:" % table_name)
            print(rows)
            # Using tabulate to show the table in a nice way
            print("Data from table %s, tabulated:" % table_name)
            print(tabulate(rows, headers=self.cursor.column_names))
            return rows
        
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
          
    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
            
def main():
    program = None
    try:
        
        program = Program()
      
        user_query = """CREATE TABLE IF NOT EXISTS %s (
                    id VARCHAR(30) NOT NULL PRIMARY KEY,
                    has_labels TINYINT NOT NULL)
                    """
        
        program.create_table(table_name="User", query=user_query)
        
        activity_query = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(30) NOT NULL,
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id))
                    """
                    
        program.create_table(table_name="Activity", query=activity_query)
        
        track_point_query = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id))
                    """
        
        program.create_table(table_name="TrackPoint", query=track_point_query)
        
        program.fetch_data(table_name="TrackPoint", limit=10)
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()



