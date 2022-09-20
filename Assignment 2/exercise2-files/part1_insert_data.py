from ast import main
import multiprocessing
from DbConnector import DbConnector
import pandas as pd
import os
import sqlalchemy
from sqlalchemy.pool import StaticPool
import glob
from part1_create_tables import *

def create_plt_df(path: str):
    plt_df = pd.read_csv(path, skiprows = 6, names =["lat", "lon", "_ignore_", "altitude", "date_days", "date", "time"])
    return plt_df

def create_transportation_labels_df(path: str):
    
    transportation_labels_df = pd.read_csv(path, sep="\t")
    transportation_labels_df['Start Time'].replace('/', '-', inplace=True, regex=True)
    transportation_labels_df['End Time'].replace('/', '-', inplace=True, regex=True)
    
    transportation_labels_df['Start Time'] = pd.to_datetime(transportation_labels_df['Start Time'], format='%Y-%m-%d %H:%M:%S')
    transportation_labels_df['End Time'] = pd.to_datetime(transportation_labels_df['End Time'], format='%Y-%m-%d %H:%M:%S')
    
    return transportation_labels_df

def create_user_df():
    user_ids = os.listdir('dataset/Data')
    
    with open('dataset/labeled_ids.txt', 'r') as f:
        labeled_user_ids = f.readlines()
        labeled_user_ids = [user_id.rstrip() for user_id in labeled_user_ids]
    
    has_labels = [True if user_id in labeled_user_ids else False for user_id in user_ids]
    
    users_df = pd.DataFrame({'id': user_ids, 'has_labels': has_labels})
    
    return users_df

#for DB
engine = sqlalchemy.create_engine('mysql+mysqlconnector://', poolclass=StaticPool, creator = lambda: DbConnector().db_connection)
activity_id = 1

def insert_data():
    
    users_df = create_user_df()
    
    try:
        users_df.to_sql("User", engine,index=False, if_exists="append")
    except Exception as e:
        print(e)
        print("[!] Already written users to db")
        
    root = 'dataset/Data'
    
    for user_id in list(users_df['id']):
        
        print("Processing user: " + user_id)
        
        global activity_id
    
        transportation_labels = create_transportation_labels_df(root+"/"+user_id+"/labels.txt") if users_df[users_df['id'] == user_id]['has_labels'].values[0] else None
        
        activities = []
        all_user_trackpoints = []
        
        for plt_path in glob.glob(root+"/"+user_id+"/Trajectory/*.plt"):
            
            activity_trackpoints = create_plt_df(plt_path)
          
            activity_trackpoints['date_time'] = activity_trackpoints['date'] + " " + activity_trackpoints['time']
            activity_trackpoints['date_time'] = pd.to_datetime(activity_trackpoints['date_time'], format='%Y-%m-%d %H:%M:%S')
            
            activity_trackpoints['activity_id'] = activity_id
            
            
            if activity_trackpoints.shape[0] <= 2500:
    
                
                start_date_time = activity_trackpoints['date_time'].iloc[0]
                end_date_time = activity_trackpoints['date_time'].iloc[-1]
                
                
                if transportation_labels is not None:
                    
                    transportation_mode = transportation_labels[(transportation_labels['Start Time'] == start_date_time) & (transportation_labels['End Time'] == end_date_time)]
                    
                    if transportation_mode.size>0:
                        transportation_mode = transportation_mode['Transportation Mode'].iloc[0]
                        
                    else:
                        transportation_mode = None
                
                else:
                    transportation_mode = None
                
                activity = [activity_id, user_id, transportation_mode, start_date_time, end_date_time]
                activities.append(activity)
                
                activity_id += 1
                
                all_user_trackpoints.append(activity_trackpoints[['activity_id','lat','lon','altitude','date_days','date_time']])
        
        user_activities = pd.DataFrame(activities, columns=['id','user_id','transportation_mode','start_date_time','end_date_time'])
       
        try:
            user_activities.to_sql("Activity", engine,index=False, if_exists="append")
        except Exception as e:
            print(e)
        
        #inserting trackpoints as batches of data
        with multiprocessing.Pool(50) as p:
            try:
                p.map(insert_trackpoints, all_user_trackpoints)
            except Exception as e:
                print(e)

def insert_trackpoints(trackpoints):
    try:
        trackpoints.to_sql("TrackPoint", engine,index=False, if_exists="append")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    insert_data()

