import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events"
staging_songs_table_drop = "DROP table if exists songplay_table"
songplay_table_drop = "DROP table if exists songplays"
user_table_drop = "DROP table  if exists users"
song_table_drop = "DROP table  if exists songs"
artist_table_drop = "DROP table if exists artists"
time_table_drop = "DROP table if exists time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE staging_events_table
                                  (se_id integer IDENTITY(0,1),
                                  artist varchar,
                                  auth varchar,
                                  first_name varchar,
                                  gender varchar,
                                  item_in_session integer,
                                  last_name varchar,
                                  length decimal,
                                  level varchar,
                                  location varchar,
                                  method varchar,
                                  page varchar,
                                  registration bigint,
                                  session_id integer,
                                  song varchar,
                                  status integer,
                                  ts bigint,
                                  user_agent varchar,
                                  user_id int);""")

staging_songs_table_create = ("""CREATE TABLE staging_songs_table 
                                (ss_id integer IDENTITY(0,1),
                                num_songs integer,
                                artist_id varchar,
                                artist_latitude numeric,
                                artist_longitude numeric,
                                artist_location varchar,
                                artist_name varchar(MAX),
                                song_id varchar,
                                title varchar,
                                duration numeric,
                                year integer);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS  songplays

                            (songplay_id integer IDENTITY(0,1),
                             start_time timestamp  distkey,
                             user_id int NOT NULL sortkey,
                             level varchar,
                             song_id varchar,
                             artist_id varchar,
                             session_id int NOT NULL,
                             location varchar,
                             user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users

                    (user_id int PRIMARY KEY NOT NULL distkey sortkey,
                     first_name varchar,
                     last_name varchar,
                     gender varchar,
                     level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs

                    (song_id varchar PRIMARY KEY NOT NULL distkey,
                     title varchar,
                     artist_id varchar sortkey,
                     year int ,
                     duration float);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                    (artist_id varchar PRIMARY KEY NOT NULL distkey ,
                    name varchar sortkey,
                    location varchar,
                    latitude float,
                    longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                    (start_time timestamp PRIMARY KEY NOT NULL distkey  ,
                     hour int,
                     day int,
                     week int,
                     month int sortkey,
                     year int ,
                     weekday varchar);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
JSON  '{}'
TIMEFORMAT AS 'epochmillisecs'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['Region']['REGION'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
JSON 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['Region']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s ON e.song = s.title AND e.artist_name = s.artist_name AND e.length = s.duration
WHERE e.page = 'NextSong'
""")
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
firstName AS first_name,
lastName AS last_name,
gender AS gender,
level AS level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id AS song_id,
title AS title,
artist_id AS artist_id,
year AS year,
duration AS duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id AS artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS latitude,
artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month,
EXTRACT(year FROM start_time) AS year,
EXTRACT(dayofweek FROM start_time) AS weekday
FROM staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
