# Created by esraa ahmed on 15/08/2022

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    
    """
    Drops each table using the queries in `drop_table_queries` list.
    
      Args:
        cur: the cursor object.
         conn = connection to Redshift DB.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
    Creates each table using the queries in `create_table_queries` list. 
    
      Args:
        cur: the cursor object.
         conn = connection to Redshift DB.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    #Read dwh config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to the host
    # Created by esraa ahmed on 15/8/2022
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #drop existing tables, and create new tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    #close connection
    conn.close()


if __name__ == "__main__":
    main()
# Created by esraa ahmed on 15/08/2022
