# Project: Data Lake

## Introduction
A music streaming startup, Sparkify, has grown their data and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, and a directory with JSON metadata on the songs in their app.

I build ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables

## Schema
I create a Relational Database with star schema. The star schema contains denormalized tables, so it will simplify queries and has fast aggrgation benefit. These advantages could allow Sparkify to do heavy reads on the database. 

#### Fact Table
**songplays** 
* *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)*

#### Dimension Tables
**users** 
* *user_id, first_name, last_name, gender, level*

**songs**
* *song_id, title, artist_id, year, duration*

**artists** 
* *artist_id, name, location, latitude, longitude*

**time**
* *start_time, hour, day, week, month, year, weekday* 
