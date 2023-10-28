    
"""ETL Operation for S3 data.

This script provide utility operations to load data from s3 bucket to staging tables,  insert data from staging
table to star scheam and print count on each table data. Utility provide following operations to achieve above
tasks
    * load_staging_tables - iterate over copy_table_queries and execute it on database object
    * insert_tables - iterate over insert_table_queries and execute it on database object
    * validate_tables - iterate over validate_queries and print the result for the same
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,validate_table_queries


def load_staging_tables(cur, conn):
    """Load data from s3 bucket to staging tables.

    Args:
        cur (_type_): Execute query on database cursor
        conn (_type_): Connection object hold database information
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging table into star schema analytics table.

    Args:
        cur (_type_): Execute query on database cursor
        conn (_type_): Connection object hold database information
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def validate_tables(cur,conn):
    """Validate that data was inserted correctly."""
    for query in validate_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        try:
            print(cur.fetchall())
        except:
            print("No results to fetch.")

def main():
    """Connect to database by using load data from s3 and insert data to .

    * loadStaging: Load data from JSON s3 bucket to staging table
    * insertTable: Fetch data from staging table and load into star schema for analysis
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    validate_tables(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()