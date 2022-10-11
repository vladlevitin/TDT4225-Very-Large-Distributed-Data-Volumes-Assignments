import glob
import os
import pandas as pd
import datetime
from DbConnector import DbConnector
from example import main




path = 'dataset/Data'


connection = DbConnector()
db = connection.db


user_collection = db.user
activity_collection = db.activity

activity_id = 1
next_trackpoint_id = 1


def create_plt_df(path: str):
    return pd.read_csv(path, skiprows=6, names=["lat", "lon", "_ignore_", "altitude", "date_days", "date", "time"], dtype=str)

def create_transportation_labels_df(path: str):
    df =  pd.read_csv(path, sep='\t',dtype=str)
    df[['Start Time','End Time']] = df[['Start Time','End Time']].apply(pd.to_datetime)
    return df
    
def assign_data_types(activity_trackpoints, next_track_point_id):
 
    activity_trackpoints['date_time'] = activity_trackpoints['date'] + ' ' + activity_trackpoints['time']
    
    activity_trackpoints[['lat','lon','date_days','altitude']] = activity_trackpoints[['lat','lon','date_days','altitude']].astype(float)
    activity_trackpoints['date_time'] = activity_trackpoints['date_time'].apply(pd.to_datetime)
    
    activity_trackpoints['_id'] = activity_trackpoints.reset_index()['index'] + next_track_point_id


def insert_data():

    users = os.listdir(path)
    users = list(filter(lambda string: string.isdigit(), users))
    users.sort()
    
    labels = {}
    
    with open(path[:-4]+'labeled_ids.txt') as f:
        user_ids = f.readlines()
        labeled_ids = [user_id.rstrip() for user_id in user_ids]
        labels = {user_id: user_id in labeled_ids for user_id in users}
        
    for user_id in users:
        
        print('Processing user: ', user_id)
        
        inserted_ids = []
        has_labels = labels[user_id]
        
        activities = []
        
        global activity_id, next_trackpoint_id
        
        if has_labels:
            transportation_modes = create_transportation_labels_df(path+"/"+user_id+'/labels.txt')
        
        for plt_path in glob.glob(path+"/"+user_id+"/Trajectory/*.plt"):
            
            activity_trackpoints = create_plt_df(plt_path)
            
        
            
            if activity_trackpoints.shape[0] <= 2500:
                
                assign_data_types(activity_trackpoints, next_trackpoint_id)
                
                start_date_time = activity_trackpoints['date_time'].iloc[0]
                end_date_time = activity_trackpoints['date_time'].iloc[-1]
                
                activity = {
                    '_id': activity_id,
                    'user_id': user_id,
                    'start_date_time': start_date_time,
                    'end_date_time': end_date_time
                }
                
                if has_labels:
                    
                    transportation_mode = transportation_modes.loc[(transportation_modes['Start Time']==start_date_time) & (transportation_modes['End Time']==end_date_time)]
                    
    
                    
                    if transportation_mode.size>0:
                        transportation_mode = transportation_mode['Transportation Mode'].iloc[0]
                    else:
                        transportation_mode = None
                        
                    
                    activity['transportation_mode'] = transportation_mode
                
                activity['TrackPoints'] = activity_trackpoints[['_id','lat','lon','altitude','date_days','date_time']].to_dict('records')
                
                activities.append(activity)
                
                activity_id += 1
                next_trackpoint_id += activity_trackpoints.shape[0]
        
        
        if activities:
            inserted_ids = activity_collection.insert_many(activities).inserted_ids
        
        user = {
            '_id': user_id,
            'activity_refs': inserted_ids
        }
        
        if has_labels:
            user['has_labels'] = has_labels
            
        user_collection.insert_one(user)

if __name__ == '__main__':
    
    
    
    # def drop_coll(collection_name):
    #     collection = db[collection_name]
    #     collection.drop()
    
    # drop_coll('user')
    # drop_coll('activity')
    
    insert_data()
