import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          TEXT ENCODE ZSTD,
    auth            TEXT ENCODE ZSTD,
    firstName       TEXT ENCODE ZSTD,
    gender          TEXT ENCODE ZSTD,
    iteminsession   INTEGER,
    lastname        TEXT ENCODE ZSTD,
    length          FLOAT,
    level           TEXT ENCODE ZSTD,
    location        TEXT ENCODE ZSTD,
    method          TEXT ENCODE ZSTD,
    page            TEXT ENCODE ZSTD,
    registration    FLOAT,
    sessionid       INTEGER,
    song            TEXT ENCODE ZSTD,
    status          INTEGER,
    ts              BIGINT,
    useragent       TEXT ENCODE ZSTD,
    userid          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id             TEXT ENCODE ZSTD PRIMARY KEY,
    title               VARCHAR(1024) ENCODE ZSTD,
    duration            FLOAT,
    year                FLOAT,
    num_songs           FLOAT,
    artist_id           TEXT ENCODE ZSTD,
    artist_name         VARCHAR(1024) ENCODE ZSTD,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR(1024) ENCODE ZSTD
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id    INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time     BIGINT  NOT NULL REFERENCES time(start_time) sortkey,
    user_id        INTEGER NOT NULL REFERENCES users(user_id),
    level          VARCHAR NOT NULL,
    song_id        VARCHAR NOT NULL REFERENCES songs(song_id) distkey,
    artist_id      VARCHAR NOT NULL REFERENCES artists(artist_id),
    session_id     INTEGER NOT NULL,
    location       VARCHAR NOT NULL,
    user_agent     VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER NOT NULL PRIMARY KEY,
    first_name  TEXT NOT NULL,
    last_name   TEXT NOT NULL,
    gender      TEXT NOT NULL,
    level       TEXT NOT NULL
)
DISTSTYLE all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     TEXT NOT NULL PRIMARY KEY,
    title       VARCHAR(1024) NOT NULL,
    artist_id   VARCHAR NOT NULL REFERENCES artists(artist_id),
    year        SMALLINT,
    duration    NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR NOT NULL PRIMARY KEY,
    name        VARCHAR(1024) NOT NULL,
    location    VARCHAR(1024),
    latitude    NUMERIC,
    longitude   NUMERIC
)
DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  BIGINT NOT NULL ENCODE RAW PRIMARY KEY,
    hour        SMALLINT,
    day         SMALLINT,
    week        SMALLINT,
    month       SMALLINT,
    year        SMALLINT,
    weekday     SMALLINT
)
DISTSTYLE all;
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    Credentials 'aws_iam_role={}'
    Format as json 'auto'
    Region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), eval(config.get('IAM_ROLE', 'ARN')))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    Credentials 'aws_iam_role={}'
    Format as json 'auto'
    Region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), eval(config.get('IAM_ROLE', 'ARN')))


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
    SELECT
        se.ts as start_time,
        se.userId as user_id,
        se.level as level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location as location,
        se.userAgent as user_agent
    FROM staging_events as se
    LEFT JOIN staging_songs as ss
    ON se.song = ss.title and se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
    AND user_id is not NULL
    AND song_id is not NULL
    AND artist_id is not NULL
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT
        DISTINCT userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id is not NULL
    ORDER BY user_id
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,
                       title,
                       artist_id,
                       year,
                       duration)
    SELECT DISTINCT song_id, title, artist_id, year::INTEGER, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
    SELECT
        DISTINCT artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
    SELECT
        start_time,
        extract(hour FROM date_time) as hour,
        extract(day FROM date_time) as day,
        extract(week FROM date_time) as week,
        extract(month FROM date_time) as month,
        extract(year FROM date_time) as year,
        extract(weekday FROM date_time) as weekday
    FROM (SELECT
                ts as start_time,
                '1970-01-01'::date + ts/1000 * interval '1 second' as date_time
          FROM staging_events
          GROUP BY start_time) as time_table
    ORDER BY start_time;
""")


# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    time_table_create,
    song_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    (staging_events_copy, 'Staging Events'),
    (staging_songs_copy, 'Staging Songs')
]

insert_table_queries = [
    (songplay_table_insert, 'Songplay'),
    (user_table_insert, 'Users'),
    (song_table_insert, 'Songs'),
    (artist_table_insert, 'Artists'),
    (time_table_insert, 'Time')
]
