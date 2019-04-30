import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function which takes an object of class Cursor to connect to the database, and executes an encapsulated query.
    Receives  the connection to the database and song data to be transformed
    into a dataframe structure for data selection and insert into the database
    Args:
        cur : Database cursor 
        filepath: Song data location
    Returns:
        void, finishes execution by executing the insert comming from sql_queries.py 
        and creating Artists and Song tables in the database
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id', 'year', 'duration']].values.tolist()
    song_data = song_data[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    artist_data = artist_data[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
     """
    Function which takes an object of class Cursor to connect to the database, and executes an encapsulated query.
    Receives  the connection to the database and Log data to be transformed
    into a dataframe structure for data selection and insert into the database
    This block transform the JSON log into a dataframe structure for data selection and insert into the database
    Args:
        cur : Cursor
        filepath: Log Data
    Returns:
        void, finishes execution by executing the insert comming from sql_queries.py and
        creating the user dimension table and songpay fact 
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = df.loc[:,'ts']
    t = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records
    time_stamp = t.values
    time_data = [time_stamp, t.dt.hour, t.dt.day,  t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['time_stamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(data=time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function which takes  parameters to connect to the database, file location and a function in turn.
    The block walks through the directory provided and takes each file one by one and is sent to the
    function required accordingly, either Song or Log
    
    Args:
        cur : Cursor to execute queries
        conn: connexion details to the database
        filepath: Log Data or song data
        func: function to be fed the file (log or song) for processing
    Returns:
        void, finishes execution by iterating over all the files from the location provided
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
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
     """
    Invokes the functions to perform ETL
    Args:
        
    Returns:
        Void, closes the connection once the ETL is done
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()