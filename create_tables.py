import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables if previously created in the redshift for avoid conflicting errors
    :param cur: current cursor for the database
    :param conn: databse on the cloud
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('Successfully Drop!')


def create_tables(cur, conn):
    """Create tables in the redshift cluster for saving result of ETL processes
    :param cur: current cursor for the database
    :param conn: databse on the cloud
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('Successfully Create Tables!')


def main():
    # Get data warehouse parameters on the redshift cluster
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop the tables in the cluster
    drop_tables(cur, conn)
    # Create table in the cluster
    create_tables(cur, conn)

    # Disconnect to the database
    conn.close()


if __name__ == "__main__":
    main()
