import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('DWH', 'DWH_ROLE_ARN')

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
CREATE TABLE staging_events
(
  se_artist        VARCHAR,
  se_auth          VARCHAR(10),
  se_first_name    VARCHAR,
  se_gender        VARCHAR(10),
  se_itemInSession SMALLINT,
  se_last_name     VARCHAR,
  se_length        DECIMAL,
  se_level         VARCHAR(6),
  se_location      VARCHAR,
  se_method        VARCHAR(6),
  se_page          VARCHAR(20),
  se_registration  BIGINT,
  se_session_id    SMALLINT,
  se_song          VARCHAR,
  se_status        SMALLINT,
  se_ts            BIGINT,
  se_user_agent    VARCHAR,
  se_user_id       SMALLINT
 );
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
  num_songs        SMALLINT,
  artist_id        VARCHAR(20),
  artist_latitude  DOUBLE PRECISION,
  artist_longitude DOUBLE PRECISION,
  artist_location  VARCHAR,
  artist_name      VARCHAR,
  song_id          VARCHAR(25), 
  title            VARCHAR,
  duration         DOUBLE PRECISION,
  year             SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays 
(
  sp_id         INT IDENTITY(0,1) NOT NULL,
  sp_start_time VARCHAR(15) NOT NULL,
  sp_user_id    INTEGER NOT NULL,
  sp_level      VARCHAR(6) NOT NULL,
  sp_song_id    VARCHAR(25) NOT NULL,
  sp_artist_id  VARCHAR(25) NOT NULL,
  sp_session_id INTEGER NOT NULL,
  sp_location   VARCHAR,
  sp_user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE users 
(
  u_id          INTEGER NOT NULL,
  u_first_name  VARCHAR NOT NULL,
  u_last_name   VARCHAR NOT NULL,
  u_gender      VARCHAR(6) NOT NULL,
  u_level       VARCHAR(6) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs
(
  s_id           VARCHAR(25) NOT NULL,
  s_title        VARCHAR NOT NULL,
  s_artist_id    VARCHAR(25) NOT NULL,
  s_year         INTEGER NOT NULL,
  s_duration     DECIMAL NOT NULL, 
  PRIMARY KEY (s_id, s_title, s_duration)
);
""")

artist_table_create = ("""
CREATE TABLE artists
(
  a_id           VARCHAR(25) NOT NULL,
  a_name         VARCHAR NOT NULL,
  a_location     VARCHAR,
  a_latitude     NUMERIC(10),
  a_longitude    NUMERIC(10)
);
""")

time_table_create = ("""
CREATE TABLE time
(
  t_start_time  TIMESTAMP NOT NULL,
  t_hour        SMALLINT NOT NULL,
  t_day         SMALLINT NOT NULL,
  t_week        SMALLINT NOT NULL,
  t_month       SMALLINT NOT NULL,
  t_year        SMALLINT NOT NULL,
  t_weekday     SMALLINT NOT NULL
);
""")


# STAGING TABLES

# Log data: s3://udacity-dend/log_data
staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json' region 'us-west-2';
""").format(ARN)

# Song data: s3://udacity-dend/song_data
staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2';
""".format(ARN)
                     )

# INSERT FOR ANALYTICAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (sp_start_time, sp_user_id, sp_level, sp_song_id, \
                       sp_artist_id, sp_session_id, sp_location, sp_user_agent)
SELECT se.se_ts             AS sp_start_time,
       se.se_user_id        AS sp_user_id,
       se.se_level          AS sp_level,
       ss.song_id           AS sp_song_id, 
       ss.artist_id         AS sp_artist_id,
       se.se_session_id     AS sp_session_id,
       se.se_location       AS sp_location,
       se.se_user_agent     AS sp_user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.se_artist = ss.artist_name) AND (se.se_song = ss.title)
WHERE se.se_artist IS NOT NULL AND se.se_song IS NOT NULL
""")

# This insert uses the data from the latest entry for each u_id.
user_table_insert = ("""
INSERT INTO users (u_id, u_first_name, u_last_name, u_gender, u_level)
SELECT u_id, u_first_name, u_last_name, u_gender, u_level
FROM
    (SELECT se.se_user_id           AS u_id,
            se.se_first_name        AS u_first_name,
            se.se_last_name         AS u_last_name,
            se.se_gender            AS u_gender,
            se.se_level             AS u_level,
            ROW_NUMBER() OVER (PARTITION BY se.se_user_id) AS user_id_ranked
     FROM staging_events se ORDER BY se.se_user_id) AS ranked
WHERE ranked.user_id_ranked = 1 AND u_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (s_id, s_title, s_artist_id, s_year, s_duration)
SELECT ss.song_id        AS s_id,
       ss.title          AS s_title,
       ss.artist_id      AS s_artist_id,
       ss.year           AS s_year,
       ss.duration       AS s_duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artists (a_id, a_name, a_location, a_latitude, a_longitude)
SELECT DISTINCT ss.artist_id    AS a_id,
    ss.artist_name              AS a_name,
    ss.artist_location          AS a_location,
    ss.artist_latitude          AS a_latitude,
    ss.artist_longitude         AS a_longitude
FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO time (t_start_time, t_hour, t_day, t_week, t_month, t_year, t_weekday)
SELECT DISTINCT
        TIMESTAMP 'epoch' + se.se_ts/1000 * interval '1 second'     AS t_start_time,
        EXTRACT(hour from t_start_time)                             AS t_hour,
        EXTRACT(day from t_start_time)                              AS t_day,
        EXTRACT(week from t_start_time)                             AS t_week,
        EXTRACT(month from t_start_time)                            AS t_month,
        EXTRACT(year from t_start_time)                             AS t_year,
        EXTRACT(dow from t_start_time)                              AS t_weekday
FROM staging_events se
""")


# QUERY LISTS

create_staging_table_queries = [staging_events_table_create, staging_songs_table_create]
create_analytical_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_analytical_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
