import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Runs the queries in copy_table_queries, which stages the data in Redshift
    :param cur: the cursor to the DB connection
    :param conn: the connection to the DB
    :return: none
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads the data staged in Redshift into analytical tables
    :param cur: the cursor to the DB connection
    :param conn: the connection to the DB
    :return: none
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main function. connects to redshift cluster, loads staging tables, then generates analytical tables
    :return: none
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    conn = psycopg2.connect(conn_string)

    cur = conn.cursor()

    # Load data from S3 buckets to staging tables
    load_staging_tables(cur, conn)
    print('Data is now staged on Redshift database!')

    # Load data from the staging tables to analytical tables
    insert_tables(cur, conn)
    print('Analytical tables are now on Redshift database!')

    conn.close()


if __name__ == "__main__":
    main()