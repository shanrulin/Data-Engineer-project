# Project: Data Modeling with AWS

## Introduction
A music streaming startup, Sparkify, has grown their data and want to move them onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, and a directory with JSON metadata on the songs in their app.

I build ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.

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
* *start_time, hour, day, week, month, year, weekday* <br/>

Timestamp Reference:

[Convert Unix epoch time into Redshift timestamps](https://dwgeek.com/convert-unix-epoch-time-into-redshift-timestamps.html/)

[Redshift Epochs and Timestamps](https://www.fernandomc.com/posts/redshift-epochs-and-timestamps/) <br/>

Note: 
Setting dimensional tables' ID fields as SORT KEY because they are (expected to be) often used in JOIN operations especially when querying the songplays table.

DISTSTYLE ALL for all dimensional tables since they are small enough to be distributed to all nodes.

## Process
Starting with `create_tables.py` script, and it creates tables using queries in `sql_queries.py`. After that, execute the `etl.py` script, and it builds ETL pipeline to first copy data from S3 to staging tables, and then insert data from staging data to Fact table and Dimension tables using queries in `sql_queries.py`


Debug note:
**stl_load_errors** is a system table in Redshift that contains records for all failed loading jobs.
You could use the statement as follow 

```select * from stl_load_errors;```
