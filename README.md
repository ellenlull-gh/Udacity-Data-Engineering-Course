## Introduction:
     This is for the Udacity Data Engineering Postgres Data modelling Project (project 1) and is an exercise to load 5 
     tables in a star schema containing music data from user listening logs and songs JSON files

    The project is to set up a Postgres database using a star schema containing 5 tables for a 
    fictitious startup called Sparkify, who wants to analyze the data they've been collecting on 
    songs and user activity on their new music streaming app.
  
### Table Overview
    Database consists of 5 tables.  
    * Fact Table:  Songplay
    * Dimension Table: Songs
    * Dumension Table: Artists
    * Dimension Table: Time
    * Dimension Table: Users
    
  ![image.png](attachment:image.png)
  

### ETL Descripton:
      The ETL will:
       1. Drop existing tables, then Create tables using a script called create_tables.py.  
           - Create_tables.py will will use statements in script sql_queries.py

       2. Loop through a series of song files which are in JSON format.   For each song record:
           - Load the Songs dimension table
           - Load the Artists dimension table
           
       3. Loop through user log files.   For each log record:
           - Load the users dimension table.  Update Level if match on user_id
           - Parse the timestamp and load a time dimension table
           - Load a songplay fact table using the log record plus data from the songs and artists dimension tables
      
 
 ## Table Metadata
 
     1.  Songplay - fact table for the steaming data.   
         - songplay_id (serial) - a sequential primary key
         - start_time (timestamp) - time when user started listening
         - user_id (integer) - Identifier of the user who is listening
         - level (varchar)  - Subscription level, paid or free
         - song_id (varchar) - identifier for the song being played
         - artist_id (varchar) - identifier for the artist of the song played
         - session_id (integer) - identifier for the session
         - location (varchar) - location of the user
         - user_agent (varchar) - application song is streamed from

    2.  User Table: Contains information on the person streaming the song
         - user_id (integer) - Identifier of the user who is listening
         - first_name (varchar) - first name of the user
         - last_name (varchar) - last name of the user
         - gender (varchar) - gender of the user
         - level (varchar) - Subscription level, paid or free

    3.  Song table:  Dimension table with data on the song:
         - song_id (varchar) - unique identifier of the song
         - title (varchar) - title of the song
         - artist_id (varchar) - unique identfier of the artist who created the song
         - year (integer) - year the song was published
         - duration (numeric) - length of play time for the song

     4. Artist table: Dimension table with data on the artist
         - artist_id (varchar) - unique identifer for the artist
         - name (varchar) - artist name
         - location (varchar) - location of the artist
         - latitude (double precision) - latitude of the artists location
         - longitude (double precision) - longitude of the artists location

      5. Time table: contains the time the user played the song
         - start_time (timestamp) - time when user started listening
         - hour (integer) -  the hour from the timestamp
         - day (integer) - the day from the timestamp
         - week (integer) - the week from the timestamp
         - month (integer) - the month from the timestamp
         - year (integer) - the year from the timestamp
         - weekday (intger) - the day of the week from the timestamp
 
         
 ## Files in Repository
 
     1. create_tables.py
      
              This script uses commands from sql_queries.py and does the following:
                   - Drops existing tables
                   - Creates the one fact and 4 dimension tables
  
               
      2. sql_queries.py
      
              This script is called by create_tables.py and contains commands to:
                   - Drop existing tables
                   - Create Tables
                   - Insert data into tables
                   - Select data from existing artist and songs tables to use in songplay fact table insert 
                  
        3. etl.py
      
              This is the main ETL script, which does the following:  
                   - Calls create_tables.py to Drop existing tables and Create Tables
                   - Reads Song JSON files
                   - For each Song record:  Load appropriate data into Songs and Artists Dimension Tables
                   - Loop through log data.   
                   - For each record in each log file, build arrays for laoding time, users and songplay tables
                   - Split time into multiple columns containing hour, day, week, month and day of week
                   - Loop through time array and load time table
                   - loop through user array and load user table.  Update level column if user_id already exists
                   - Look up song, artist and duration in song and artist dimension tables.  Build Songplay record
                   - Insert record into songplay table.
                   - Close Connection to database
                This script reads the SQL table insert statements and the SQL select for the song and artist table lookup
                from the sql_queries.py script described above
                
                                  
        4. etl.ipynb
        
              This is a jupyter notebook version of etl.py.  It can also be run to load the fact and dimension tables:  
                   - Calls create_tables.py to Drop existing tables and Create Tables
                   - Reads Song JSON files
                   - For each Song record:  Loads panda dataframe for Songs and Artists Dimension Tables.  
                   - Loop through song data array and perform insert for Song dimension table
                   - Loop through song data array and permorm insert for Artist dimension table
                   - Loop through log data and load it into a pandas dataframe 
                   - For each record in each log file, build arrays for laoding time, users and songplay tables
                   - Split time into multiple columns containing hour, day, week, month and day of week
                   - Loop through time array.  Load time table.   
                   - loap through user array.  Load user table.  If user_id already exists in table, update level column
                   - loop through user array and load user table.  Update level column if user_id already exists
                   - Build Songplay dataframe
                   - Loop through songplay dataframe.  Look up Look up song, artist and duration in song and artist 
                     dimension tables.  Insert record into songplay table.
                   - Close Connection to database
     
 
 ## How to Execute ETL 
     To run the ETL through the python script:
       1.  Open a terminal session
       2.  Type "python etl.py"
    
    To run the ETL through the Jupyter Notebook
       1.  Open the etl.ipynb
       2.  From the Run pulldown menu, click "Run all cells"
           
 
