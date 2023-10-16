"""
@author  : Joy Pedze
@Email   : joyp.pedze@gmail.com
@Date    : 16 October 2023
"""

# import needed libraries
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

# get password from environment variable for postgres database access
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

# sql server database details
driver = "{SQL Server}"
server = "localhost"
database = "AdventureWorksDW2022"
tcn = "Yes"

# extract data from sql server
def extract():
    try:
        src_conn = pyodbc.connect('Trusted_connection=' + tcn + ';DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' + ';DATABASE=' + database)
        # cursor allows python code to execute sql command in a database session
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute(""" select t.name as table_name 
        from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}',src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

# load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks')
        print(f'importing {rows_imported} rows to {rows_imported + len(df)}... for table {tbl}')
        # save the df to postgres
        df.to_sql(f'stg_{tbl}',engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
