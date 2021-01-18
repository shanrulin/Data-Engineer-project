import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
ARN = config.get("IAM_ROLE","ARN")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events ( 
  artist         VARCHAR,
  auth           VARCHAR,
  firstName      VARCHAR,
  gender         VARCHAR,
  itemInSession  INTEGER,
  lastName       VARCHAR,
  length         float,
  level          VARCHAR,
  location       VARCHAR,
  method         VARCHAR,
  page           VARCHAR,
  registration   NUMERIC,
  sessionid      INTEGER,
  song           VARCHAR,
  status         INTEGER,
  ts             BIGINT,
  userAgent      VARCHAR,
  userid         INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  num_songs        INTEGER,
  artist_id        VARCHAR,
  artist_latitude  float,
  artist_longitude float,
  artist_location  VARCHAR,
  artist_name      VARCHAR,
  song_id          VARCHAR,
  title            VARCHAR,
  duration         float,
  year             INTEGER
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                              songplay_id    INTEGER IDENTITY(0,1) PRIMARY KEY, 
                              start_time     timestamp NOT NULL REFERENCES time(start_time),
                              user_id        int NOT NULL REFERENCES users(user_id), 
                              level          varchar, 
                              song_id        varchar NOT NULL REFERENCES songs(song_id), 
                              artist_id      varchar NOT NULL REFERENCES artists(artist_id), 
                              session_id     varchar, 
                              location       varchar, 
                              user_agent     varchar)
                              SORTKEY AUTO;""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                          user_id        int PRIMARY KEY SORTKEY, 
                          first_name     varchar, 
                          last_name      varchar, 
                          gender         varchar, 
                          level          varchar)
                          diststyle all;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                           song_id       varchar PRIMARY KEY SORTKEY, 
                           title         varchar,
                           artist_id     varchar NOT NULL, 
                           year          int, 
                           duration      float)
                           diststyle all;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id    varchar PRIMARY KEY SORTKEY, 
                            name         varchar, 
                            location     varchar,
                            latitude     float, 
                            longitude    float)
                            diststyle all;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                           start_time   timestamp PRIMARY KEY SORTKEY, 
                           hour         int, 
                           day          int, 
                           week         int, 
                           month        int, 
                           year         int, 
                           weekday      int)
                           diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
format as JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto' truncatecolumns;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * interval '1 second' as start_time,
                e.userid,
                e.level,
                s.song_id,
                s.artist_id,
                e.sessionid,
                e.location,
                e.useragent
FROM staging_events AS e
JOIN staging_songs  AS s
  ON (e.artist=s.artist_name) AND 
     (e.song=s.title) AND
     (e.length=s.duration)
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid,
                firstName,
                lastName,
                gender,
                level
FROM staging_events e
WHERE e.page = 'NextSong' AND
      user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
                EXTRACT(HOUR FROM start_time)  as hour,
                EXTRACT(DAY FROM start_time)   as day,
                EXTRACT(WEEK FROM start_time)  as week,
                EXTRACT(MONTH FROM start_time) as month,
                EXTRACT(YEAR FROM start_time)  as year,
                EXTRACT(DOW FROM start_time)   as weekday
FROM
(SELECT DISTINCT start_time
FROM songplays)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
