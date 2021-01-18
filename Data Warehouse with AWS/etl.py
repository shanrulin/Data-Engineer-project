import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    ''' Load data from S3 to staging tables'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    ''' Insert data from staging tables to Fact/Dimension tables'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    1. Load data to staging tables
    2. Insert data from staging tables to Fact/Dimension tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
