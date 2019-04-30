#!/usr/bin/env python
# coding: utf-8

# ### Project 2 - Cassandra Data Modeling
# #### William Galindez Arias
# 
# 
# Goal: Learn the foundational principles of data modeling in NoSQL Databases. In this project Cassandra is used to learn the fundamental approach of QUERIES FIRST and model the data tables to answer the query. In addition, put into practice the theory around Primary Key, partition key, clustering and Where Clauses and how the proper selection of this parameters allows queries and results without 'Allow Filtering'
# 
# 
# In each of the queries, the business question is answered, the tables are modeled after the query using only the data relevant to it, and leveraging the Partition Keys, Compound Keys and Clustering concepts to achieve the desired data distribution and order (Partition, Clustering)

# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import Python packages 

# In[5]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[9]:


print(os.getcwd())
filepath = os.getcwd() + '/event_data'
for root, dirs, files in os.walk(filepath):
    file_path_list = glob.glob(os.path.join(root,'*'))


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[10]:



full_data_rows_list = [] 
    

for f in file_path_list:

    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
               
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[11]:


with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Apache Cassandra. 
# 
# ### <font color=red>event_datafile_new.csv</font>.  contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Apache Cassandra Steps

# #### 1. Creating a Cluster

# In[1]:


from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect()


# #### 2. Create Keyspace

# In[2]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### 3. Set Keyspace

# In[3]:


try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# #### 4. Model Tables after Required Queries
# 
# 
# ##### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ##### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ##### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# ### 5. Tables Creation: Solution

# 1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

# ##### Table 1
# 
# `
# Table Name: music_log 
# column 1: Artist Name
# column 2: song title
# column 3: song length
# Column 4: sessionId
# column 5: itemInSession
# PRIMARY KEY(session_id, item_session)`
# 

# Load the event csv File and create a Dataframe with it

# In[8]:


df = pd.read_csv('event_datafile_new.csv')
df.tail()


# Create a dictionary to store the index of each column in the DataFrame 'df'

# In[10]:


key_number = {}
keys_list = df.keys()
for counter, key in enumerate(keys_list):
    key_number[key] = counter
print(key_number)


# #### Table 1 Creation in Cassandra Database, where as Primary Key is used SessionId as partition key and itemInSession as clustering

# In[11]:


query = "CREATE TABLE IF NOT EXISTS artist_song_length "
query = query + "(session_id int, item_session int, artist_name text, song_title text, song_length decimal, PRIMARY KEY (session_id, item_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)


# 
# 
# #### Data insertion into the table created above, with its corresponding column value and data type
#                     

# In[13]:



file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:

        query = "INSERT INTO artist_song_length (session_id, item_session, artist_name, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s )"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


# #### Query 1:    SELECT artist_name, song_title, song_length from  udacity.artist_song_length WHERE session_id=338 AND item_session =4 

# In[65]:


query_read = "SELECT artist_name, song_title, song_length from  udacity.artist_song_length WHERE session_id=338 AND item_session =4 "
try:
    rows = session.execute(query_read)
except Exception as e:
    print(e)
    


# #### Query 1 Result

# In[66]:


for row in rows:
    print(row, '\n')
    dict_1 = {'artist_name':row.artist_name, 'song_title':row.song_title, 'song_length': row.song_length }
    print (' Artist Name: ', row.artist_name,'\n', 'Song Title: ', row.song_title,'\n','Song Length: ', row.song_length)


# #### Query 1 Output DataFrame

# In[76]:


df_q1 = pd.DataFrame(dict_1, index=[0])
df_q1.head()


# ### -----------------------------------------------------------------\\ ------------------------------------------------------------------

# #### Table 2 Creation in Cassandra Database, where Compounded Key is used with
# ####   Selected UserId and SessionId as partition key  and itemInSession as clustering

# In[34]:


query_2 = "CREATE TABLE IF NOT EXISTS artist_song_session "
query_2 = query_2 + "(user_id int, session_id int, item_session int, first_name text, last_name text, artist_name text, song_title text,PRIMARY KEY ((user_id, session_id), item_session))"
try:
    session.execute(query_2)
except Exception as e:
    print(e)


# 
# #### Data insertion into the table 2 created above, with its corresponding column value and data type

# In[35]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO artist_song_session (user_id,  session_id, item_session,  first_name, last_name, artist_name, song_title)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[1], line[4], line[0], line[9]))


# #### Query 2:    SELECT first_name, last_name, artist_name,  song_title from  udacity.artist_song_session WHERE user_id=10 AND session_id =182

# In[101]:


query_2_read = "SELECT first_name, last_name, artist_name,  song_title from  udacity.artist_song_session WHERE user_id=10 AND session_id =182 "
try:
    rows_2 = session.execute(query_2_read)
except Exception as e:
    print(e)

                    


# In[102]:


artists_names, songs_titles = [],[]
for row in rows_2:
    print(row, '\n')
    artists_names.append(row.artist_name)
    songs_titles.append(row.song_title)
    print (' User First  Name: ', row.first_name,'\n', 'User Last Name: ', row.last_name,'\n','Artist Name: ', row.artist_name, '\n', 'Song Title: ', row.song_title, '\n')
    

dict_2 = {'first_name':row.first_name, 'last_name':row.last_name, 'artist_name': artists_names, 'song_title': songs_titles }


# #### Query 2 Output DataFrame

# In[111]:


print(' user_id=10 & session_id =182: ', '\n', dict_2['first_name'], dict_2['last_name'], '\n')
d = {'Artist Name' : dict_2['artist_name'], 'Song Title': dict_2['song_title'] }
df_q2 = pd.DataFrame(d)
df_q2.head()


# ### ----------------------------------------------------------\\ ---------------------------------------------------------------

# 
# #### Table 3 Creation in Cassandra Database, where Partition Key is song title
# #### with user_id  as clustering, with this choosing is possible to  ensure uniqueness of the data
# 
#                     

# In[45]:


query_3 = "CREATE TABLE IF NOT EXISTS app_history_song_listeners "
query_3 = query_3 + "(song_title text, user_id int, first_name text, last_name text, PRIMARY KEY ((song_title), user_id))"
try:
    session.execute(query_3)
except Exception as e:
    print(e)


# 
# #### Data insertion into the table 3 created above, with its corresponding column value and data type

# In[46]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) 
    for line in csvreader:
        query = "INSERT INTO app_history_song_listeners (user_id, first_name, last_name, song_title)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))


# #### Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# ##### SELECT first_name, last_name, artist_name,  song_title from  udacity.artist_song_session WHERE user_id=10 AND session_id =182

# In[113]:


query_3_read = "SELECT first_name, last_name from  udacity.app_history_song_listeners WHERE song_title='All Hands Against His Own'  "
try:
    rows_3 = session.execute(query_3_read)
except Exception as e:
    print(e)


# In[114]:


dict_3 = {'User First Name': [], 'User Last Name':[]}
for row in rows_3:
    print(row, '\n')
    dict_3['User First Name'].append(row.first_name)
    dict_3['User Last Name'].append(row.last_name)
    print (' User First  Name: ', row.first_name,'\n', 'User Last Name: ', row.last_name,'\n')


# #### Query 3 Output DataFrame (User First and Last Name)

# In[117]:


df_q3 = pd.DataFrame(dict_3)
df_q3.head()


# ### 6. Tables DROP

# In[20]:


query = "DROP TABLE artist_song_length"
try:
    rows = session.execute(query)
    print('Success')
except Exception as e:
    print(e)


# In[32]:


query_2 = "DROP TABLE artist_song_session"
try:
    rows = session.execute(query_2)
    print('Success')
except Exception as e:
    print(e)


# In[41]:


query_3 = "DROP TABLE app_history_song_listeners"
try:
    rows = session.execute(query_3)
    print('Success')
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[ ]:


session.shutdown()
cluster.shutdown()

