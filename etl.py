import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time


def load_staging_tables(cur, conn):
    for query, name in copy_table_queries:
        print('Start to copy {} table from S3...'.format(name))
        start = time.time()
        cur.execute(query)
        conn.commit()
        print('Complete with {} sec!'.format(round(time.time() - start, 2)))


def insert_tables(cur, conn):
    for query, name in insert_table_queries:
        print('Start to insert into {} table...'.format(name))
        start = time.time()
        cur.execute(query)
        conn.commit()
        print('Complete with {} sec!'.format(round(time.time() - start, 2)))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
