"""
file: create_schema.py
date: 10/23/20
input: no input needed #NOTE: input could be created to create specific database schemas
function: to create database, table, schema for drugbank.com
output: none. #NOTE: outputs can be created to send to log
"""

import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def get_connection(dbname = "postgres"):
    user = "postgres"
    password = "n%:Wx{n%<ygk;7d^" # NOTE: Keys can be stored in a keys directory, ex: `../project_name/keys`, (ignored by .git) in the same project but preferably in a secure directory in the server handled by the database administrator. 
    connect_str = "dbname={} host='localhost' user='{}' password='{}'".format(dbname, user, password)

    return psycopg2.connect(connect_str)

def main():
    """
    Creation of PostgreSQL database, tables and views using psycopg2
    """

    # Create a tuple of dictionaries containing the SQL to create database, tables and views
    # NOTE: Queries for SQL can be separated into different directory.
    queries = ({"Description": "Create database",
                "Database": "postgres",
                "SQL": "CREATE DATABASE drugbank"},

               {"Description": "Create drugs table",
                "Database": "drugbank",
                "SQL": "CREATE TABLE drugs(drugbankid varchar(64) NOT NULL PRIMARY KEY, smiles varchar NOT NULL)"},

               {"Description": "Create targets table",
                "Database": "drugbank",
                "SQL": "CREATE TABLE targets(targetid serial PRIMARY KEY, drugbankid varchar REFERENCES drugs(drugbankid) NOT NULL, actions varchar(64) NOT NULL, gene_name varchar(256) NOT NULL)"},

               {"Description": "Create alternative_ids table",
                "Database": "drugbank",
                "SQL": "CREATE TABLE alternative_ids(alt_id serial PRIMARY KEY, drugbankid varchar REFERENCES drugs(drugbankid) NOT NULL, alt_ids varchar(128) NOT NULL)"}         
    )

    # Run database/schema queries
    try:
        for query in queries:
            conn = get_connection(query["Database"])
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()

            cursor.execute(query["SQL"])

            print("Executed {}".format(query["Description"])) # NOTE: log what database and table/schema are created

            cursor.close()
            conn.close()

    except psycopg2.ProgrammingError as e:
        print(e) #NOTE: Log when error occurs

## Functionality
main() 

    
