from datetime import timedelta
from math import dist
from operator import index
from sqlalchemy import create_engine, text
import pandas as pd

import vyuha.postgres_conn as pg

# # #
db = pg.Redshift('rakeshpanigrahy', '86RjNJz7zQ3thVTjGF6KpJKr', 'prod', 'dl3.ahwspl.net', 5439)


def get_dl3_connection():
    conn = create_engine('redshift://rakeshpanigrahy:86RjNJz7zQ3thVTjGF6KpJKr@dataplatform.cozflcbz62sl.ap-south-1.redshift.amazonaws.com:5439/prod')
    return conn

def get_distributor_names():
    sql_query = "select fa.name from adhoc.flash_asset fa order by 1"
    t = text(sql_query)
    conn = get_dl3_connection()
    df = pd.read_sql(t, conn)
    return df

def get_distributor_names_id():
    sql_query = "select fa.asset_id, fa.name from adhoc.flash_asset fa order by 1"
    t = text(sql_query)
    conn = get_dl3_connection()
    df = pd.read_sql(t, conn)
    return df

def getQuery(sqlname, start_date, end_date, unit_asset_id=None, dest='vyuha/sql/'):
    try:
        with open(dest + sqlname, 'r') as f:
            query = f.read()
    except:
        print("SQL file read error. Make sure it exists in sql/ with the correct name.")
        exit()
    print("Query created.")
    return query.format(start_date=start_date, end_date=end_date, asset_id=unit_asset_id)

def fetch4rmDL3(query):
    conn = get_dl3_connection()
    print("Connected to DL3. Executing query")
    df = pd.read_sql(query, conn)
    print("Query results fetched.")
    conn.close()
    return df

def get_cluster_sales_data(start_date, end_date, unit_asset_id, is_export_as=None):
    conn = get_dl3_connection()
    
    query = getQuery('cluster_sales_data.sql', start_date, end_date, unit_asset_id)
    t = text(query)
    print(t)
    conn = get_dl3_connection()
    df = pd.read_sql(t, conn)

    if is_export_as != None:
        output_file = 'vyuha/distance_matrix/output_files/sales/{}.csv'.format(is_export_as)
        print(output_file)
        df.to_csv(output_file, index=False)
    return df


def exceltodl3(df, tablename, todelete=True, pagesize=10000, conditions={}):
    print('doing excel file to dl3 table')

    print('data read' + str(df.shape))
    
    if df.shape[0] > 0 and todelete == 1 and len(conditions) > 0:
        counter = 0
        q = f'''delete from {tablename}'''
        for key, val in conditions.items():
            if counter == 0:
                q = q + f''' where {key} = '{val}' '''
            else:
                q = q + f''' and {key} = '{val}' '''
            counter =+ 1
        db.execute(q)

    db.execute_vals(query=f'insert into {tablename} values %s', data=df, pagesize=pagesize)
    print('data inserted')