import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"


# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE STAGING_EVENTS
    (
       artist varchar distkey,
       auth varchar,
       firstName varchar,
       gender varchar,
       itemInSession varchar,
       lastName varchar,
       length double precision,
       level varchar,
       location varchar,
       method varchar,
       page varchar,
       registration varchar,
       sessionId integer,
       song varchar sortkey,
       status integer,
       ts BIGINT,
       userAgent varchar,
       userId integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE STAGING_SONGS
    (
        num_songs integer,
        artist_id varchar,
        artist_latitude decimal(10),
        artist_longitude decimal(10),
        artist_location varchar,
        artist_name varchar distkey,
        song_id varchar,
        title varchar sortkey,
        duration DECIMAL(10),
        year integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE SONGPLAYS 
    (
        songplay_id integer IDENTITY(0,1) not null,
        start_time timestamp not null sortkey,
        user_id integer not null,
        level varchar(5),
        song_id varchar(50) not null distkey,
        artist_id varchar(50) not null,
        session_id integer not null,
        location varchar(100),
        user_agent varchar(500)
    )
""")

user_table_create = ("""
    CREATE TABLE USERS 
    (
        user_id integer not null sortkey,
        first_name varchar(50) not null,
        last_name varchar(80) not null,
        gender varchar(1) not null, 
        level varchar(15)
    )
    diststyle all
""")

song_table_create = ("""
    CREATE TABLE SONGS 
    (
        song_id varchar(50) not null sortkey distkey,
        title varchar(500) not null,
        artist_id varchar(50) not null,
        year integer not null,
        duration DECIMAL(10)
   )
""")

artist_table_create = ("""
    CREATE TABLE ARTISTS
    (
        artist_id varchar(50) not null sortkey,
        name varchar(500) not null,
        location varchar(500),
        latitude DECIMAL(10),
        longitude DECIMAL(10)
    )
    diststyle all
""")

time_table_create = ("""
    CREATE TABLE TIME
    (
        start_time timestamp not null sortkey,
        hour smallint not null,
        day smallint not null,
        week smallint not null,
        month smallint not null,
        year smallint not null,
        weekday SMALLINT not null
    )
    diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
    copy STAGING_EVENTS from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    format as json 's3://udacity-dend/log_json_path.json'
    region 'us-west-2';
""").format(ARN)

staging_songs_copy = ("""
    copy STAGING_SONGS from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO SONGPLAYS(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    (
        SELECT 
            DISTINCT TIMESTAMP 'epoch' + ST.ts/1000 * INTERVAL '1 second'  AS start_time, 
            ST.userId, 
            ST.level,
            SO.song_id,
            SO.artist_id,
            ST.sessionId,
            ST.location,
            ST.userAgent
        FROM STAGING_EVENTS ST
        JOIN STAGING_SONGS SO ON ST.artist = SO.artist_name
        WHERE ST.page = 'NextSong'
    )
""")

user_table_insert = ("""
    INSERT INTO USERS(user_id,first_name,last_name,gender,level)
    (
        SELECT userId,firstName,lastName,gender,level FROM STAGING_EVENTS 
        WHERE userId in (SELECT DISTINCT(userId) FROM STAGING_EVENTS)
    )
""")

song_table_insert = ("""
    INSERT INTO SONGS(song_id,title,artist_id,year,duration)
    (
        SELECT song_id,title,artist_id,year,duration FROM STAGING_SONGS 
        WHERE song_id in (SELECT DISTINCT(song_id) FROM STAGING_SONGS)
    )
""")

artist_table_insert = ("""
    INSERT INTO ARTISTS(artist_id,name,location,latitude,longitude)
    (
        SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM STAGING_SONGS 
        WHERE artist_id in (SELECT DISTINCT(artist_id) FROM STAGING_SONGS)
    )
""")

time_table_insert = ("""
    INSERT INTO TIME(start_time,hour,day,week,month,year,weekday)
    (
        SELECT
            DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year from start_time) AS year,
            EXTRACT(week FROM start_time) AS weekday
        FROM STAGING_EVENTS
        WHERE page = 'NextSong'
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

count_staging_events = ("""SELECT COUNT(*) FROM STAGING_EVENTS""")
count_staging_songs = ("""SELECT COUNT(*) FROM STAGING_SONGS""")
count_songplays = ("""SELECT COUNT(*) FROM SONGPLAYS""")
count_user = ("""SELECT COUNT(*) FROM USERS""")
count_songs = ("""SELECT COUNT(*) FROM SONGS""")
count_artists = ("""SELECT COUNT(*) FROM ARTISTS""")
count_time = ("""SELECT COUNT(*) FROM TIME""")

validate_table_queries = [count_staging_events,count_staging_songs,count_songplays,count_user,count_songs,count_artists,count_time]