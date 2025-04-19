import psycopg2
import psycopg2.extras
import pandas as pd
import sys, os

sys.path.append(os.getcwd().replace('\\etl_components',''))

from config import settings


def return_connection_details():
    conn = psycopg2.connect(f"host='localhost' port=5432 dbname='USDA_DB' user={settings.pgadmin_user} password={settings.pgadmin_password}")

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


def populate_data(df, table, conn, cur):
    if len(df) > 0:
        df_columns = list(df)

        columns = ",".join(df_columns)

        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 

        insert_stmt = "INSERT INTO {} ({}) {}".format(table,columns,values)
        cur.execute("truncate " + table + ";")  # avoiding uploading duplicate data!
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
    conn.commit()