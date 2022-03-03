##  Script loads tables from songs and music log JSON files.    Loops through all song files and loads them into the
##  songs and artist tables.    Then loops through each user log file and loads to Users and Time tables. 
##  Creates songplay fact table using user log table and performing lookups on the song and artist tables.

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

##  Run the create_tables.py script
os.system('python create_tables.py')

def process_song_file(cur, filepath):
    """
    Description: This function is responsible for processing each song file.  It runs for each JSON file.  The JSON file
    is read.  Next the data for the Songs dimension data is put into a dataframe and converted to a list.  The list is
    loaded into the songs dimension table using an insert statment read from sql_queries.py. Next additional data from the 
    songs file is read into an artist dataframe and converted to a list.  The list is loaded into the artists dimension
    table

    Arguments:
        cur: the cursor object.
        filepath: log data or song data file path.

    Returns:
        None
    """
    
    # open song file

    df = pd.read_json(filepath,lines=True)
    songdf = df
        
    ## Replace any NaN with 0 for numeric 

    songdf['duration'] = pd.to_numeric(songdf['duration']).fillna(0)
    songdf['year'] = pd.to_numeric(songdf['year']).fillna(0)
    songdf['artist_latitude'] = pd.to_numeric(songdf['artist_latitude']).fillna(0)
    songdf['artist_longitude'] = pd.to_numeric(songdf['artist_longitude']).fillna(0)

    ## Columns to extract: song_id, title, artist_id, year, duration
 
    song_df = songdf[['song_id','title','artist_id','year','duration']]
    song_data=song_df.values.tolist()

    # insert song record
    cur.execute(song_table_insert, song_data[0])
   
    ## Columns to extract: artist_id, name, location, latitude, longitude
    artist_df = songdf[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    artist_data=artist_df.values.tolist()
    
    # insert artist record
    cur.execute(artist_table_insert, artist_data[0])



def process_log_file(cur, filepath):
    """
    Description: This function is responsible for processing each log file.  It runs for each JSON file.  The JSON file
    is read and the individual records are put into a dataframe.  Next the timestamp is broken up into its components 
    which are placed in a dataframe and converted to a list.  This list is looped through and data is inserted into the time
    dimension table.  Next the user table data is placed in a dataframe which is converted to a list and these records are 
    inserted into the user table replacing the value for a level when a user_id already exists  Finally a dataframe
    for the songplay fact table is created.  It is augmented by querying the songs and artists table using a select
    statement read from the sql_queries.py script.   This dataframe is converted to a list.  This list is looped through to
    insert table into the songplay table. Data is inserted using insert staments read from sql_queries.py.  

    Arguments:
        cur: the cursor object.
        filepath: log data or song data file path.

    Returns:
        None
    """
    # open log file

    logdf = pd.read_json(filepath,lines=True)
 
    ## Replace any NaN with 0 for numeric 

    logdf['userId'] = pd.to_numeric(logdf['userId']).fillna(0)

    # convert timestamp column to datetime
    time_df = logdf[['ts']]
    t= pd.to_datetime(time_df['ts'], unit='ms')
    
    #Time table colums: start_time, hour, day, week, month, year, weekday)")
    time_data = pd.DataFrame(list(zip(t, t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.dayofweek)),
               columns =['start_time', 'hour','day','week','month','year','weekday'])

    time_data2=time_data.values.tolist()
 
    # insert time data records
    for timerecord in time_data2:
        cur.execute(time_table_insert,timerecord)
        
    # load user table
    ## Columns to extract: user_id, first_name, last_name, gender, level 
    
    user_df = logdf[['userId','firstName','lastName','gender','level']]
    user_data=user_df.values.tolist()
    for userdata in user_data:
        cur.execute(user_table_insert,userdata)

    # insert user records
    cur.execute(user_table_insert,user_data[0])

    # insert songplay records
    # get songid and artistid from song and artist tables

    for index, row in logdf.iterrows():
        cur.execute(song_select,[row.song, row.length, row.artist])
        results = cur.fetchone()
           
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        t= pd.to_datetime(row.ts, unit='ms')
        songplay_data = (t, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        
  
def process_data(cur, conn, filepath, func):
    """
    Description: This function lists the files in a directory and then runs a function to process the file and 
    load the data into appropriate tables.
 
    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: funtion that either transforms songs or logs data and loads appropriate tables.

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    songdf = pd.DataFrame()
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
 
    """
    Description: This is the main driver function for the script.   It calls the process_data function for either
    the songs data or the log data
 
    Arguments:
        None

    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    import create_tables

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    
    main()