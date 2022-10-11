import glob
import os
import pandas as pd
import datetime
from DbConnector import DbConnector


names = ["lat", "lon", "_ignore_",
         "altitude", "date_days", "date", "time"]

activity_labels = ['_id','transportation_mode','start_date_time','end_date_time']
trackpoint_labels = ['_id','lat','lon','altitude','date_days','date_time']



connection = DbConnector()
db = connection.db

user_collection = db.user
activity_collection = db.activity

activity_id = 1
next_trackpoint_id = 1


def create_plt_df(path: str):
    return pd.read_csv(path, skiprows=6, names=names,dtype=str)

def create_transportation_labels_df(path: str):
    df =  pd.read_csv(path, sep='\t',dtype=str)
    # replace '/' with '-'
    #df['Start Time'] = df['Start Time'].str.replace('/', '-')
    #df['End Time'] = df['End Time'].str.replace('/', '-')
    
    df[['Start Time','End Time']] = df[['Start Time','End Time']].apply(pd.to_datetime)
    
def assign_data_types(activity_trackpoints, next_track_point_id):
 
    activity_trackpoints['date_time'] = activity_trackpoints['date'] + ' ' + activity_trackpoints['time']
    
    activity_trackpoints[['lat','lon','date_days','altitude']] = activity_trackpoints[['lat','lon','date_days','altitude']].astype(float)
    activity_trackpoints['date_time'] = activity_trackpoints['date_time'].apply(pd.to_datetime)
    
    activity_trackpoints['_id'] = activity_trackpoints.reset_index()['index'] + next_trackpoint_id


def insert_data():

    users = os.listdir('dataset/Data')
    users = list(filter(lambda string: string.isdigit(), users))
    users.sort()
    
    labels = {}
    
    with open('dataset/labels.txt') as f:
        user_ids = f.readlines()
        labeled_ids = [user_id.rstrip() for user_id in user_ids]
        labels = {user_id: user_id in labeled_ids for user_id in users}