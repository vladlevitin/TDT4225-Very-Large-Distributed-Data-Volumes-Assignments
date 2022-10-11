import datetime
from pprint import pprint
from DbConnector import DbConnector
import pandas as pd
import haversine
import part1

class Part2Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def show_result(self, query_result):
        for document in query_result:
            print(document)

    def get_one(self, query_result, col_name):
        for document in query_result:
            return document[col_name]
    
    # Part 2.1
    #  How many users, activities and trackpoints are there in the dataset (after it is inserted into the database)
    
    def part_1(self):
        
        amount_users = self.db.user.count_documents({})
        amount_activities = self.db.activity.count_documents({})

        amount_trackpoints = self.db.activity.aggregate([
            {"$group": {"_id": "TrackPoints", "count": {
                "$sum": {"$size": "$TrackPoints"}}}}
        ])

        print("{" + f"'users':{amount_users}" + "}")
        print("{" + f"'activities':{amount_activities}" + "}")
        self.show_result(amount_trackpoints)

    

        
def main():
    program = None
    try:
        program = Part2Program()

        print("Part 2.1:", sep="\n")
        program.part_1()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
