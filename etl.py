# Created by esraa ahmed on 15/8/2022

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
        Function is used to load data from s3 to the staging tables in Redshift using COPY command

    Args:
        cur: the cursor object.
         conn = connection to Redshift DB.
    """
        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
        Function is used to load data from staging tables to the final tables

    Args:
        cur: the cursor object.
         conn = connection to Redshift DB.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
     #Read dwh config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to the host
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Run SQL queries 
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    #close connection
    conn.close()


if __name__ == "__main__":
    main()

 # Created by esraa ahmed on 15/8/2022   
