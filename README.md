
## Introduction
The purpose of this project is to build the Data Warehouse on the Cloud,
specifically Redshift cluster on the AWS, and to help the startup Sparkify for its
analytical goals of several departments including marketing, management, and etc.
The main idea is to analyze user activities throughout the time. Forthurmore, the
database should be highly functionable and also efficient to process those data
for the company.

## Source Data
There are mainly two sources `SONG_DATA` AND `LOG_DATA` with Amazon S3 URL
`s3://udacity-dend/song_data` and `s3://udacity-dend/log_data` respectively. To load
data from Amazon S3 to the staging tables created at Redshift cluster, it is then
easier for the company to do any ETL processes further. The format of data in both
buckets are `JSON`.

## Database Schema
Creating and designing the database with star schema. There are 5 tables in total (one fact
table and four dimension tables) showing below:
### Fact Table
    - songplays
        * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
    - users
        * user_id, first_name, last_name, gender, level
    - songs
        * song_id, title, artist_id, year, duration
    - artists
        * artist_id, name, location, lattitude, longitude
    - time
        * start_time, hour, day, week, month, year, weekday

## How to Run
- Direct to the correct location where this Data Warehouse Project saved in the terminal
with command like `cd`
- Update the configure file dwh.cfg with correct setting of your AWS Redshift cluster.
If you open configure file, you would find `CLUSTER`, `IAM_ROLE`, and `AWS` need to be fill in. The
`AWS` and `DWH` fields is only used for creating redshift in the Jupyter Notebook (IaC).
- Run `Python create_table.py` in the terminal and check status.
- Run `Python etl.py` in the terminal and check status.
- After steps above, it is time to do any data analysis parts.

## Data Analysis
ETL pipline help business users more easily to understand information in the database and then
transform into insights to potential stakeholders ans customers. In this data warehouse, it is
possible to analyze user behaviors like who is the most populer artist and which song is
the most popular currently.

Please find the `Ad-hoc` queries and visualizations in the `jupyter notebook`.

