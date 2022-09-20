class SQLQueries:
    def __init__(self) -> None:
        
        # How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        self.task1 = {table_name: f'SELECT COUNT(*) FROM {table_name};' for table_name in ['User', 'Activity', 'TrackPoint']}
        
        self.task2_init = """
        CREATE TEMPORARY TABLE ActivityCount
        
        SELECT user_id, COUNT(*) AS count 
        FROM Activity
        GROUP BY user_id
        ORDER BY count DESC;
        """
        # Find the average number of activities per user.       
        self.task2 = """SELECT AVG(count) AS average_activities
        FROM ActivityCount;"""
        
        # Find the top 20 users with the highest number of activities
        self.task3 = """ SELECT user_id, count
        FROM ActivityCount
        LIMIT 20;
        """
        
        # Find all the users who have taken a taxi
        self.task4 = """SELECT DISTINCT user_id from Activity WHERE transportation_mode = 'taxi';"""
        
        #Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null
        self.task5 = """SELECT transportation_mode, COUNT(*) AS count 
        from Activity WHERE transportation_mode IS NOT NULL 
        GROUP BY transportation_mode;"""
        
from haversine.haversine import haversine_vector
from DbConnector import DbConnector
from tabulate import tabulate

import pandas as pd
import haversine
import part1_create_tables
import part1_insert_data
import numpy as np

class Program:
        def __init__(self):
            self.connection = DbConnector()
            self.db_connection = self.connection.db_connection
            self.cursor = self.connection.cursor
        
        def fetch_data(self, query, print_results=True):
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            if print_results:
                print(tabulate(rows, headers=self.cursor.column_names), end="\n"*2)
            return rows

    

def main():
    
    program = Program()
    all_queries = SQLQueries()
    
    try:
            # print("\nTask 2.1:\n")
            # for table_name, query in all_queries.task1.items():
            #     print(f"Table: {table_name}\n")
            #     program.fetch_data(query, table_name)
                

            # print("\nTask 2.2:")
            # print("Average number of activties per user:\n")
            # program.cursor.execute(all_queries.task2_init)
            # program.fetch_data(all_queries.task2)
            
    
            # print("\nTask 2.3:")
            # print("Top 20 users with most activities:\n")
            # program.fetch_data(all_queries.task3)
            
            # print("\nTask 2.4:")
            # print("Users who have taken a taxi:\n")
            # program.fetch_data(all_queries.task4)
            
            print("\nTask 2.5:")
            print("Types of transportation modes and their activities count:\n")
            program.fetch_data(all_queries.task5)
            
                
                
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()
    
        
    

if __name__ == "__main__":
    main()