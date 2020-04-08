# Sparkify Data Lake

## Overview
A startup with a music streaming app service wants to analyze the data they have been collecting on 
songs and its user activities on the application. 
The data contains the information on what music its uers listened to and other information on the user, songs and artists.
Throughout this database, the company can have an easy access to the structured data on the needed information. 

## Datalake schema

In the database, the schema used to organize the data is the star schema. It has a fact table of songplays which contains information on the log data of song plays.
Then, it has 4 dimension tables: users, songs, artists and time.
Through this schema, the database is normalized and organized in such a way that its users can efficiently query the data.

### Fact Table
* **songplays**: records in log data associated with song plays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimenion Tables
* **users**: users in the app (user_id, first_name, last_name, gender, level)
* **songs**: songs in the music database (song_id, title, artist_id, year, duration)
* **artists**: artists in the music database (artist_id, name, location, lattitude, longitude)
* **time**: timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)


## Files
* `elt.py` contains python code to perform the ETL process: 1) Extracting json data objects in the given S3, 2) Transforming the data with Spark, 3) Loading the processed data as parquet files into a new S3 


## How to run the codes

Since the language used in the project is Python3, it needs to be installed. Also, pyspark needs to be installed (https://spark.apache.org/docs/latest/api/python/pyspark.sql.html).
Also, since this code utilizes AWS S3 for data, AWS account is necessary.
Then, create a file called 'dl.cfg' and fill in as below:

```
[AWS]
ACCESS_KEY=<yaccess_key>
SECRET_KEY=<secret_key>
```
Also, S3 bucket needs to be created to store all the paquet output files of the code.
After creating the S3 bucket, simple replace the output_data in the main() function with the name of the created S3 bucket.


With all the above ready, on your command line,
Type `python3 etl.py` 


## Example queries

Some of the exame queries are as below.
For example, 

* Get users who listened to certain songs at a particular year and month


```
SELECT  sp.songplay_id,
        u.user_id,
        s.song_id,
        u.last_name,
        a.name,
        s.title
FROM songplays AS sp
        JOIN users   AS u ON (u.user_id = sp.user_id)
        JOIN songs   AS s ON (s.song_id = sp.song_id)
        JOIN time    AS t ON (t.start_time = sp.start_time)
WHERE t.year = '2018' AND t.month = '11'
LIMIT 100;
```
