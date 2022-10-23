import datetime
from pprint import pprint
from DbConnector import DbConnector
import pandas as pd
from haversine import haversine
import part1
from dateutil import parser
import isodate

class Tools:

    def __init__(self):
        pass

    # Calculates the total distance traveled from multiple lat long points
    def odometer(self, df):
        distance = 0

        # Iterate through activity IDs
        for idx in df.id.unique():
            activity_df = df[df.id == idx].copy()

            # Insert end coordinates
            activity_df['lat_end'] = activity_df['lat'].shift(-1)
            activity_df['lon_end'] = activity_df['lon'].shift(-1)

            # Remove last record (has no end)
            activity_df.drop(activity_df.tail(1).index, inplace=True)

            # Calculate distance and sum up for each row
            for _, row in activity_df.iterrows():
                distance += haversine((row.lat, row.lon), (row.lat_end, row.lon_end), unit="km")

        return distance


class Part2Program:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.tools = Tools()

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

    def part_2(self):

        activity_average = self.db.user.aggregate([
            {"$group": {"_id": None,
                        "Average_activities": {"$avg": {"$size": "$activity_refs"}}}},
            {"$project": {"_id": 0, "Average_activities": {"$round": ["$Average_activities", 3]}}}
        ])

        self.show_result(activity_average)

    def part_3(self):
        """
        Find the top 20 users with the most activities.
        As the users are allready in a collection, the refs can just be summes up per user."""

        most_activities = self.db.user.aggregate([
            {"$project": {"_id": 1, "Total_activities": {"$size": "$activity_refs"}}},
            {"$sort": {"Total_activities": -1}},
            {"$limit": 20},
        ])

        self.show_result(most_activities)

    def part_4(self):
        """Find all users that have taken a taxi"""
        """
        Lookup is for matching entries based on fields
        Unwind is for creating a new duplicate row per array element in a single row
        
        """

        taken_taxi = self.db.user.aggregate([
            # Unwind the activity refs into their own row
            {"$unwind": {"path": "$activity_refs"}},
            # Join activity collection:
            {"$lookup": {"from": "activity",
                         "localField": "activity_refs",
                         "foreignField": "_id",
                         "as": "activity"}},
            {"$project": {"_id": 1, "activity.transportation_mode": 1}},
            {"$match": {"activity.transportation_mode": "taxi"}},
            {"$group": {"_id": "$_id"}},
            {"$sort": {"_id": 1}}
        ])
        self.show_result(taken_taxi)

    def part_5(self):
        """
        Find all types of transportation modes and count how many activities
        that are tagged with each transportation mode
        Null rows are ignored
        """
        transportation_mode_activities = self.db.activity.aggregate([
            {"$match": {"transportation_mode": {"$exists": "true", "$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "activities": {"$count": {}}}},
            {"$sort": {"activities": 1}}
        ])

        self.show_result(transportation_mode_activities)

    def part_6a(self):
        """
        Find the year with the most activities.
        """
        year_with_most_activities = self.db.activity.aggregate([
            {"$group":
                 {"_id":
                      {"$toInt":
                           {"$year": "$start_date_time"}
                       }
                     ,
                  "activities": {"$count": {}}
                  }
             },
            {"$sort": {"activities": -1}},
            {"$limit": 1}
        ])
        self.show_result(year_with_most_activities)

    def part_6b(self):
        """
        Find the year with the most recorded hours.
        """
        year_with_most_hours = self.db.activity.aggregate([
            {"$group":
                 {"_id":
                      {"$toInt":
                           {"$year": "$start_date_time"}
                       }
                     ,
                  "activities": {"$sum": {"$dateDiff": {
                      "startDate": "$start_date_time",
                      "endDate": "$end_date_time",
                      "unit": "hour"
                  }}}
                  }
             },
            {"$sort": {"activities": -1}},
            {"$limit": 1}
        ])
        self.show_result(year_with_most_hours)


    def part_7(self):
        """
        Find the year with the most recorded hours.
        Projects the year to query based on it
        """
        track_points = self.db.activity.aggregate([
            {"$project": {"user_id": 1, "year": {"$year": "$start_date_time"}, "transportation_mode": 1, "TrackPoints": 1}},
            {"$unwind": {"path": "$TrackPoints"}},
            {"$match": {
                "$and": [
                    {"user_id": {"$eq": "112"}}, {"year": {"$eq": 2008}}, {"transportation_mode": {"$eq": "walk"}},
            ]}},
        ])

        # Cleaning from JSON format to DataFrame
        df = pd.DataFrame(list(track_points))
        df = df.rename(columns={"_id": "id"})
        df = df.join(pd.json_normalize(df.TrackPoints))
        df = df.drop(columns=["TrackPoints"])
        print(self.tools.odometer(df))


def main():
    program = None
    try:
        program = Part2Program()

        # print("Part 2.1:", sep="\n")
        # print("Total amount of users, activities and trackpoints in the dataset")
        # program.part_1()

        # print("\nPart 2.2:", sep="\n")
        # print("Find the average number of activities per user")
        # program.part_2()

        # print("\nPart 2.3:", sep="\n")
        # print("Find top 20 users with the highest number of activities")
        # program.part_3()

        # print("\nPart 2.4:", sep="\n")
        # print("Find all users who have taken a taxi")
        # program.part_4()

        # print("\nPart 2.5:", sep="\n")
        # print("Find activities per transportation mode")
        # program.part_5()

        #print("\nPart 2.6a:", sep="\n")
        #print("Find the year with the most activities")
        #program.part_6a()

        print("\nPart 2.7:", sep="\n")
        print("Find the year with the most recorded hours")
        program.part_7()


    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
