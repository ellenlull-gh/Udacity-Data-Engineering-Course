###  
##  Description:    This is part of the POSTGRES Data Modelling Project (Project 1) for UDACITY Data Engineering course
##                  This project is an exercise to load 5 tables in a star schema containing music data from user 
##                  listening logs and songs JSON files
##
##                  The project is to set up a Postgres database using a star schema containing 5 tables for a 
##                  fictitious startup called Sparkify, who wants to analyze the data they've been collecting on 
##                  songs and user activity on their new music streaming app.
##
##                  This script is called by create_tables.py and contains commands to:
##                      Drop existing tables
##                      Create Tables
##                      Insert data into tables
##                      Select data from existing artist and songs tables to use in songplay fact table insert 

##
# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays \
                        ( \
                          songplay_id SERIAL UNIQUE PRIMARY KEY, \
                          start_time TIMESTAMP NOT NULL, \
                          user_id INT NOT NULL,\
                          level VARCHAR, \
                          song_id VARCHAR, \
                          artist_id VARCHAR, \
                          session_id INT, \
                          location VARCHAR, \
                          user_agent VARCHAR) \
                         ")

user_table_create = ("CREATE TABLE IF NOT EXISTS users \
                      (  \
                        user_id INT PRIMARY KEY, \
                        first_name VARCHAR, \
                        last_name VARCHAR, \
                        gender VARCHAR, \
                        level VARCHAR) \
                      ")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs \
                      ( \
                         song_id VARCHAR NOT NULL, \
                         title VARCHAR NOT NULL, \
                         artist_id VARCHAR, \
                         year INT, \
                         duration NUMERIC NOT NULL, \
                         PRIMARY KEY(song_id)) \
                     ")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists \
                        ( \
                          artist_id VARCHAR, \
                          name VARCHAR, \
                          location VARCHAR, \
                          latitude DOUBLE PRECISION, \
                          longitude DOUBLE PRECISION, \
                          PRIMARY KEY(artist_id)) \
                        ")

time_table_create = ("CREATE TABLE IF NOT EXISTS time \
                      (  \
                        start_time TIMESTAMP PRIMARY KEY NOT NULL, \
                        hour INT, \
                        day INT, \
                        week INT, \
                        month INT, \
                        year INT, \
                        weekday INT) \
                     ")

# INSERT RECORDS


songplay_table_insert = ("INSERT INTO songplays \
                           ( \
                             start_time, \
                             user_id,\
                             level, \
                             song_id, \
                             artist_id, \
                             session_id, location, user_agent) \
                             VALUES(%s,%s,%s,%s,%s,%s,%s,%s) \
                           ")

user_table_insert = ("INSERT INTO users \
                      (  \
                         user_id, \
                         first_name, \
                         last_name, \
                         gender, \
                         level) \
                         VALUES(%s,%s,%s,%s,%s) ON CONFLICT (user_id) \
                         DO UPDATE SET level = EXCLUDED.level \
                       ")


song_table_insert = ("INSERT INTO songs \
                      ( \
                        song_id, \
                        title, \
                        artist_id, \
                        year,  \
                        duration) \
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING \
                     ")

artist_table_insert = ("INSERT INTO artists \
                        ( \
                          artist_id, \
                          name, \
                          location, \
                          latitude, \
                          longitude \
                         ) \
                          VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING \
                       ")

time_table_insert = ("INSERT INTO time \
                      ( \
                        start_time, \
                        hour, \
                        day, \
                        week, \
                        month, \
                        year, \
                        weekday\
                        ) \
                        VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING \
                     ")

# FIND SONGS
# Select used for loading songplay fact table.  

song_select = ("SELECT \
                 song_id, \
                 songs.artist_id \
                 FROM songs \
                 JOIN artists on songs.artist_id = artists.artist_id \
                 WHERE title = %s and duration = %s and name=%s")

# QUERY LISTS
## used in script create_tables.py
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]