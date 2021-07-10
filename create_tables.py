import configparser
import psycopg2

from sql_queries import create_staging_table_queries, create_analytical_table_queries, \
     drop_staging_table_queries, drop_analytical_table_queries


def drop_tables(cur, conn):
    """
    Drops the staging tables and the analytical tables
    :param cur: the cursor to the DB connection
    :param conn: the connection to the DB in the redshift cluster
    :return: none
    """
    for query in drop_staging_table_queries:
        cur.execute(query)
        conn.commit()

    for query in drop_analytical_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the staging tables and analytical tables
    :param cur: the cursor to the DB connection
    :param conn: the connection to the DB in the redshift cluster
    :return: none
    """
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()

    for query in create_analytical_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    conn = psycopg2.connect(conn_string)

    cur = conn.cursor()

    print('Dropping existing tables.')
    drop_tables(cur, conn)

    print('Creating new tables...')
    create_tables(cur, conn)
    print('Tables created!')

    conn.close()


if __name__ == "__main__":
    main()