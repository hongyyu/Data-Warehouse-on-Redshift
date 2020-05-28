import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    """Copying data from S3 bucket into the staging tables in the redshift cluster
    and then use for dimention and fact tables
    :param cur: current cursor for the database
    :param conn: databse on the cloud
    """
    for query, name in copy_table_queries:
        print('Start to copy {} table from S3...'.format(name))
        start = time.time()
        cur.execute(query)
        conn.commit()
        print('Complete with {} sec!'.format(round(time.time() - start, 2)))


def insert_tables(cur, conn):
    """Insert data from staging table in the cluster into dimension and fact tables
    in the appropriate types
    :param cur: current cursor for the database
    :param conn: databse on the cloud
    """
    for query, name in insert_table_queries:
        print('Start to insert into {} table...'.format(name))
        start = time.time()
        cur.execute(query)
        conn.commit()
        print('Complete with {} sec!'.format(round(time.time() - start, 2)))


def main():
    # Get data warehouse parameter in the cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database in the cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    # Copy data into staging tables
    load_staging_tables(cur, conn)
    # Insert data into dimension and fact tables
    insert_tables(cur, conn)

    # Disconnect to the database in the cluster
    conn.close()


if __name__ == "__main__":
    main()
