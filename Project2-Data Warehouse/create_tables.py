
"""Create and Delete Operations for Staging and Star Schema tables.

This script provide utility operations to create tables for star schema and also perform drop operations for the same.
Utility provide following operations to achieve this task
    * drop_tables - iterate over drop_table_queries and execute the operation
    * create_table - iterate create_table queries and execute the operation
    * main - main function of the script
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Iterate over drop table queries and execute the same on database object.

    Args:
        cur (_type_): Execute query on database cursor
        conn (_type_): Connection object hold database information
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Iterate over create table queries and execute the same on database object.

    Args:
        cur (_type_): Execute query on database cursor
        conn (_type_): Connection object hold database information
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to database and execute drop table and create table.

    * drop_tables: Drop the tables by taking drop table as list
    * create_tables: Create the tables by taking create tables as list
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()