# AWS Cloud Data Warehouse with Amazon Redshift

## Motivation
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  

An ETL pipeline is built that extracts their data from S3, stages them in Redshift, and transforms data into a set of analytical tables 
for their analytics team to continue finding insights in what songs their users are listening to.  

## Implementation
A Postgres database will be created using a star schema with fact and dimension tables. Python will be used to write the ETL pipeline, 
which will access the json files (for the songs and for the user acivity) from S3 buckets, stage this data on Amazon Redshift,
and insert this data into the relevant analytical tables.

### Database Design
As mentioned, a star schema will be employed here. 

#### Staging tables
* **staging_events** contains all data from the user activity JSON logs
* **staging_songs** contains all song metadata from JSON files 

#### Fact table
* **songplays** will serve as the fact table and will (among other attributes) contain foreign keys for the following dimension tables.

#### Dimension table
* **users** contains information about Sparkify users and has the following fields: *user_id, first_name, last_name, gender, level* (paid for free)
* **songs** contains information about Sparkify songs and has these fields: *song_id, title, artist_id year, duration* (length of song)
* **artists** contains information about music artists and has these fields: artist_id, name, location, latitude, longitude
* **time** contains the components of the timestamps of the log data, having these fields: *start_time, hour, day, week, month, year, weekday*  

This schema is used to facilitate business matric analysis strictly through the fact table. 
If it's desired to know how many unique songs are played per hour, how many unique artists are played in a given time window, etc., 
this can all be carried out by queries on the **songplays** fact table. For further details about *which* unique artist is played 
the most in a given time window, or *when* a specific song title is played most, then the dimension tables must be accessed. 

### Database creation
The database is created by running the *create_tables.py* file. This file does the following:
1. Connects to an existing database on an already running Redshift cluster
2. Drops all tables that might already exist
3. Creates all required tables (the ones listed above) 

The SQL queries used in *create_tables.py* can be found in the *sql_queries.py* file.  
  
  
### ETL pipeline
The ETL pipeline is contained in the *etl.py* file. It contains the following steps:

#### 1. Pull and copy data in JSON files
The song metadata JSON files are stored in an S3 bucket here: s3://udacity-dend/song_data/  
Each of the JSON files is copied into a into the **staging_songs** table.  

The user activity JSON logs are stored in an S3 bucket at this location: s3://udacity-dend/log_data/  
Each of the JSON files is copied into a into the **staging_events** table.

#### 2. Generate analytical tables
The data in the **staging_songs** table is used to fill in the **songs** and **artists** tables. 

Some special notes on the `INSERT` query handling:
* Since an artist can have multiple songs, the `INSERT` query, which does a `SELECT` on the **staging_songs** table, 
uses a DISTINCT condition so that there are no duplicate entires for the same artist.

* Since songs will need to be uniquely identified by the combination of their titles and durations (since something like a *song_id* is not available in the log data), the rows in the **songs** table will have a composite `PRIMARY KEY` containing *s_id*, *s_title*, and *s_duration*.  
  
Next, the data in the **staging_events** table is used to fill in the **songplays**, **users**, and **time** tables.  

Special note on the `INSERT` query handling:
* User data can change over time (as an example, user's subscription status might change from paid to free). The query used here, which does a `SELECT` on the **staging_events** table, selects only the last inserted entry with the same *u_id* (the user ID). This ensures that the most recent data about the user is entered into the table.  


## How to Run
1. Create an IAM role with AmazonRedshiftFullAccess and AmazonS3ReadOnlyAccess. 
2. Create a Redshift cluster that is associated with the IAM role in step 1.
3. Enter the AWS session key and secrete key, the IAM role ARN, and the Redshift cluster information into the *dwh.cfg* file.  
4. Run *create_tables.py* to create the tables.
5. Run *etl.py* to run the ETL pipeline.
  
  
## Example Analysis
Here are some example queries that can be run on the database.  

TOTAL NUMBER OF STREAMS:

    SELECT SUM(songplays.sp_id) FROM songplays
    
*Output*:  
sum  
56656

STREAMING COUNTS BY ACCOUNT LEVEL:  

    SELECT songplays.sp_level, SUM(songplays.sp_id) 
                           FROM songplays 
                           GROUP BY songplays.sp_level
*Output*:  
sp_level,sum  
paid,45819  
free,10837  

TOP 5 USERS WITH MOST LISTENS 

    SELECT users.u_first_name || ' ' || users.u_last_name u_full_name, SUM(songplays.sp_user_id) 
                         FROM (songplays JOIN users ON songplays.sp_user_id=users.u_id) 
                         GROUP BY u_full_name 
                         ORDER BY SUM(songplays.sp_user_id) DESC LIMIT 5

*Output*:  
u_full_name sum  
Kate Harrell	3104  
Tegan Levine	2480  
Chloe Cuevas	2058  
Mohammad Rodriguez	1496  
Jacob Klein	1314  

TOP 5 STREAMING DAYS OF THE MONTH  

    SELECT time.t_day, SUM(songplays.sp_id) 
                                FROM (songplays JOIN time ON songplays.sp_start_time=time.t_start_time) 
                                GROUP BY time.t_day 
                                ORDER BY SUM(songplays.sp_id) DESC LIMIT 5  
                                
*Output*:  
t_day  sum  
5	5571  
15	4573  
29	3750  
30	3100  
21	3069  
