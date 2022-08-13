import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "Drop table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "Drop table if exists users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(artist        VARCHAR,
                                                                            auth          VARCHAR,
                                                                            firstName     VARCHAR,
                                                                            gender        VARCHAR,
                                                                            ItemInSession int,
                                                                            lastName      VARCHAR,
                                                                            length        DOUBLE PRECISION,
                                                                            level         VARCHAR,
                                                                            location      Text,
                                                                            method        VARCHAR,
                                                                            page          VARCHAR,
                                                                            regestration  DOUBLE PRECISION,
                                                                            SessionId     int,
                                                                            song          VARCHAR,
                                                                            status        int,
                                                                            ts            TIMESTAMP,
                                                                            userAgent     VARCHAR,
                                                                            userId        int
                                                                            );  """)

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs( song_id          VARCHAR,
                                                                            num_songs        int,
                                                                            artist_id        int,
                                                                            artist_name      VARCHAR,
                                                                            title            VARCHAR,
                                                                            artist_latitude  DOUBLE PRECISION,
                                                                            artist_longitude DOUBLE PRECISION,
                                                                            artist_location  TEXT,
                                                                            duration         int,
                                                                            year             int
                                                                            );
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays( songplay_id int IDENTITY(0,1) NOT NULL PRIMARY KEY, 
                                                                  start_time timestamp NOT NULL,
                                                                  user_id int NOT NULL, 
                                                                  level VARCHAR,
                                                                  song_id int NOT NULL,
                                                                  artist_id int NOT NULL,
                                                                  session_id int,
                                                                  location TEXT,
                                                                  user_agent VARCHAR)
                                                                  
                                                                  DISTKEY(song_id)
                                                                  SORTKEY(start_time);

""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users( user_id int NOT NULL PRIMARY KEY,
                                                          first_name VARCHAR NOT NULL,
                                                          last_name VARCHAR NOT NULL,
                                                          gender VARCHAR,
                                                          level VARCHAR )
                                                          SORTKEY(user_id);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS song(song_id VARCHAR NOT NULL PRIMARY KEY,
                                                         title VARCHAR NOT NULL,
                                                         artist_id int NOT NULL,
                                                         year int NOT NULL,
                                                         duration int NOT NULL)
                                                               SORTKEY(song_id);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist(artist_id VARCHAR NOT NULL PRIMARY KEY,
                                                             name VARCHAR NOT NULL,
                                                             location VARCHAR,
                                                             lattitude DOUBLE PRECISION,
                                                             longitude DOUBLE PRECISION)
                                                             SORTKEY(artist_id);

""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP NOT NULL PRIMARY KEY,
                                                         hour int,
                                                         day int,
                                                         week int,
                                                         month int,
                                                         year int,
                                                         weekday int)
                                                         SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events from {}
                           credentials 'iam_role= {}'
                           region 'us-east-1'; 
""").format(config.get('S3', 'LOG_DATA'), 
            config.get('IAM_ROLE','ARN'), 
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" COPY staging_songs from {}
                           credentials 'iam_role= {}'
                           region 'us-east-1';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays(start_time, user_id, level, song_id, artist_id,
                          session_id, location, user_agent)
                          
                          SELECT DISTINCT  timestamp 'epoch' + (ste.ts/1000) * interval '1 second' AS start_time,
                          ste.user_id, ste.level, sts.song_id, sts.artist_id, 
                          ste.SessionId, ste.location, ste.userAgent
                          FROM staging_events ste
                          JOIN staging_songs sts ON (ste.song = sts.title AND 
                                                     ste.artist = sts.artist_name
                                                     AND ste.length = sts.duration)
                          WHERE ste.page = 'NextSong';                      
""")

user_table_insert = (""" INSERT INTO users(user_id, first_name, last_name, gender, level)

                         SELECT DISTINCT ste.userId, ste.firstName, ste.lastName, ste.gender, ste.level
                         FROM staging_events ste
                         WHERE userId IS NOT NULL;
""")

song_table_insert = (""" INSERT INTO song(song_id, title, artist_id, year, duration)

                         SELECT DISTINCT sts.song_id, sts.title, sts.artist_id, sts.year, sts.duration
                         FROM staging_songs sts
                         WHERE song_id IS NOT NULL;
""")

artist_table_insert = (""" INSERT INTO artist(artist_id, name, location, latitude, longitude)

                           SELECT DISTINCT sts.artist_id, sts.artist_name AS name, sts.artist_location AS location
                           sts.artist_latitude AS latitude, sts.artist_longitude AS longitude
                           FROM staging_songs sts;
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)

                         SELECT DISTINCT timestamp 'epoch' + (ste.ts/1000) * interval '1 second' AS start_time,
                         EXTRACT(hour FROM start_time)     AS hour,
                         EXTRACT(day FROM start_time)      AS day,
                         EXTRACT(week FROM start_time)     AS week,
                         EXTRACT(month FROM start_time)    AS month,
                         EXTRACT(year FROM start_time)     AS year
                         EXTRACT(week FROM start_time)     AS weekday
                         FROM staging_events ste
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
