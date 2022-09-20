from haversine.haversine import haversine_vector
from DbConnector import DbConnector
from tabulate import tabulate

import pandas as pd
from haversine import haversine
import part1_create_tables
import part1_insert_data
import numpy as np



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

        # Find the year with the most activities. Ordering by the activity count and limiting query result to the first element: the year with the most activities.
        # Perhaps check for duplicates in start time for same user?
        self.task6a = """SELECT EXTRACT(YEAR FROM start_date_time) AS start_year, 
                                COUNT(EXTRACT(YEAR FROM start_date_time)) AS COUNT 
                         FROM Activity GROUP BY start_year ORDER BY COUNT DESC LIMIT 1;"""

        # Is 2008 the year with the most recorded hours too?
        # How to find time difference between two dates
        self.task6b = """SELECT EXTRACT(YEAR FROM start_date_time) AS start_year, 
                                SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as hours 
                         FROM Activity GROUP BY start_year ORDER BY hours DESC LIMIT 1;"""

        # Total distance (in km) walked in 2008, by user id=112
        # where walking and user id = 112
        # Use havershine
        # Alot of missing transportation mode values, ignoring them
        # Sum haversine distance for each activity!
        self.task7 = """SELECT a.user_id, t.date_time, a.id, t.lat, t.lon FROM Activity a
                        INNER JOIN TrackPoint t ON t.activity_id = a.id
                                WHERE EXTRACT(YEAR FROM start_date_time) = '2008' AND user_id = 112 AND transportation_mode = 'walk'
                                """

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

        def fetch_data_with_columns(self, query, print_results=True):

            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            if print_results:
                print(tabulate(rows, headers=self.cursor.column_names), end="\n" * 2)
            return rows, self.cursor.column_names

        # Calculates the total distance traveled from multiple lat long points
        def odometer(self, df):
            distance = 0

            # Iterate through activity IDs
            for idx in df.id.unique():
                activity_df = df[df.id == idx].copy()

                #Insert end coordinates
                activity_df['lat_end'] = activity_df['lat'].shift(-1)
                activity_df['lon_end'] = activity_df['lon'].shift(-1)

                # Remove last record (has no end)
                activity_df.drop(activity_df.tail(1).index, inplace=True)

                # Calculate distance and sum up for each row
                for _, row in activity_df.iterrows():
                    distance += haversine((row.lat, row.lon), (row.lat_end, row.lon_end), unit="km")

            return distance

    

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
            
            #print("\nTask 2.5:")
            #print("Types of transportation modes and their activities count:\n")
            #program.fetch_data(all_queries.task5)
            
            #print("\nTask 2.6a:")
            #print("The year with the most activities:\n")
            #program.fetch_data(all_queries.task6a)

            #print("\nTask 2.6b:")
            #print("The year with the most recorded hours:\n")
            #program.fetch_data(all_queries.task6b)

            print("\nTask 2.7:")
            print("Total distance walked in 2008 by user with id=112:\n")
            track_points, columns = program.fetch_data_with_columns(all_queries.task7, print_results=False)
            track_points = pd.DataFrame(track_points, columns=columns)
            print(program.odometer(track_points))

    except Exception as e:
        print("ERROR: Failed to use database:", e)
        
    finally:
        if program:
            program.connection.close_connection()
    
        
    

if __name__ == "__main__":
    main()