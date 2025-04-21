'''
Load.py

- Loads the unioned dataframe into PostgreSQL using library psycopg2.
- Creates the schema for the landing table, which psycopg2 execute_batch can insert data at once using a connection string object.
- conn.autocommit will treat statements as individual transactions to be committed against the database.

'''

import psycopg2
import psycopg2.extras
import sys, os

from psycopg2.extras import execute_values

sys.path.append(os.getcwd().replace('\\etl_components',''))
from config import settings

DB_NAME = 'USDA_DB'
DB_USER = settings.pgadmin_user
DB_PASS = settings.pgadmin_password

def return_connection_details():
    conn = psycopg2.connect(database= DB_NAME, \
                        user= DB_USER, \
                        password= DB_PASS, \
                        host = 'host.docker.internal', \
                        port='5432')


    conn.autocommit=True
    cur = conn.cursor()
    return conn, cur


def create_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS ag.CropPrices; 
            
    CREATE UNLOGGED TABLE ag.CropPrices (
        county_name				text,
        watershed_desc			text,
        statisticcat_desc		text,
        group_desc				text,
        Value					real,
        freq_desc				text,
        zip_5					integer,
        short_desc				text,
        state_name				text,
        state_fips_code			text,
        commodity_desc			text,
        load_time				text,
        agg_level_desc			text,
        week_ending				text,
        domaincat_desc			text,
        begin_code				text,
        state_alpha				text,
        location_desc			text,
        congr_district_code		text,
        state_ansi				text,
        country_name			text,
        asd_code				text,
        domain_desc				text,
        end_code				text,
        prodn_practice_desc		text,
        id						text,
        reference_period_desc	text,
        asd_desc				text,
        watershed_code			text,
        CV_Percentage			real,
        county_code				text,
        region_desc				text,
        util_practice_desc		text,
        county_ansi				text,
        year					integer,
        unit_desc				text,
        class_desc				text,
        sector_desc				text,
        source_desc				text,
        country_code			text
    )           
    """)


def populate_data(df, table, conn):
    columns = ', '.join(df.columns)
    data = tuple(tuple(row) for row in df.values) #
    cur = conn.cursor()


    SQL = f"INSERT INTO {table} ({columns}) VALUES %s;"

    execute_values(cur, SQL, data)
    conn.commit()