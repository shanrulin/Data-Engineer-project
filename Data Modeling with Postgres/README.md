# Project: Data Modeling with Postgres

## Introduction
A startup company called Sparkify is interested in understanding what songs users are listening to. However, the company don't have an easy way to query their data, which resides in a directory on user activity on the app in JSON format and a directory with JSON metadata on the songs in their app. The goal of this project is to model user activity data to create a database using Postgres and build ETL pipeline with Python for the music streaming app.

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

<img src="https://github.com/shanrulin/Data-Engineer-project/blob/main/ER.png" width="800" height="700">

## Files to build database and ETL
1. `create_tables.py`: a main Python script to create needed tables
2. `sql_queries.py`: all the sql queries to create tables, insert data and find needed data
3. `etl.ipynb`: contains detail process to build database and ETL with single file
4. `etl.py`: a Python script to build database and ETL for all files
5. `test.ipynb`: a notebook to check if database built correctly
6. `README.md`: a summary of the project

## Process
Starting with `create_tables.py` script, and it creates tables using queries in `sql_queries.py`. After that, execute the `etl.py` script, and it builds ETL pipeline to insert data to tables using queries in `sql_queries.py`


