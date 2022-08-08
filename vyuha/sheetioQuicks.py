import os
import math
import psycopg2
import pandas as pd
from time import sleep
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import gspread
import boto3
# import sqlalchemy as db
import io
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from httplib2 import Http
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt
from sshtunnel import SSHTunnelForwarder

#uses Shameer's credentails by default. To create your own credentials use this link: https://developers.google.com/sheets/api/quickstart/python
def apiconnect(token='vyuha/token.json', client_json='vyuha/client_id.json'):
    SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    store = file.Storage(token)
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets(client_json, SCOPES)
        creds = tools.run_flow(flow, store)
    driver = build('drive', 'v3', http=creds.authorize(Http()))
    sheeter = build('sheets', 'v4', http=creds.authorize(Http()))
    return driver, sheeter

def makePGConn(dbHost, dbPort, dbUser, dbPwd, dbName):
    return psycopg2.connect("host={} port={} dbname={} user={} password={}"
                            .format(dbHost, dbPort, dbName, dbUser, dbPwd))

## DL2 ####################################################
def getDL2():
    return makePGConn('dl2.ahwspl.net', 5439, os.environ['DbUser'], os.environ['DbPassword'], 'warehouse')
    # return makePGConn('dl2.ahwspl.net', 5439, 'shameer', 'shameer123', 'warehouse')

def fetch4rmDL2(query):
    conn = getDL2()
    print("Connected to DL2. Executing query")
    df = pd.read_sql(query, conn)
    print("Query results fetched.")
    conn.close()
    df.to_csv('test_sample.csv')
    return df

## DL3 ####################################################
def getDL3():
    return makePGConn('dl3.ahwspl.net', 5439, os.environ['DbUser'], os.environ['DbPassword'], 'prod')
    
def fetch4rmDL3(query):
    conn = getDL3()
    print("Connected to DL3. Executing query")
    df = pd.read_sql(query, conn)
    print("Query results fetched.")
    conn.close()
    return df

## DGSKULL ####################################################
def getdgskull():   
    # ssh variables
    host = 'bastion.digihealth'
    ssh_username = 'dgjump'
    ssh_private_key = 'dgjump.pem'
    
    # database variables
    localhost = 'dg-prod-redshift.cra6no306u9h.ap-south-1.redshift.amazonaws.com'
    user=os.environ['DbUser']
    password=os.environ['DbPassword']
    database='dgskull'
    
    server =  SSHTunnelForwarder(
        ('bastion.ahwspl.net', 22),
        ssh_username='ascentvpn',
        ssh_private_key='vpn.pem',
        remote_bind_address=('dg-prod-redshift.cra6no306u9h.ap-south-1.redshift.amazonaws.com', 5439)
    )
    
    print('ssh tunnel forwarder server')
    server.start()
    print('server start')
    return makePGConn(
        'localhost', 
        server.local_bind_port,
        user,
        password,
        database
    )

def fetch4rmdgskull(query):  
    conn = getdgskull()
    print("Connected to dgskull. Executing query")
    df = pd.read_sql(query, conn)
    print("Query results fetched.")
    conn.close()
    df.to_csv('test_sample.csv')
    return df
    
    print('connected')
    return pd.read_sql_query(query, conn)

## -------- ####################################################

def getQuery(sqlname, d_date, dest='sql/'):
    try:
        with open(dest + sqlname, 'r') as f:
            query = f.read()
    except:
        print("SQL file read error. Make sure it exists in sql/ with the correct name.")
        exit()
    print("Query created.")
    return query.format(d=d_date)

def getQueryD(sqlname, d_date,distributor_id=''):       # same as getQuery but with additional argument for distributor id
    try:
        with open('sql/' + sqlname, 'r') as f:
            query = f.read()
    except:
        print("SQL file read error. Make sure it exists in sql/ with the correct name.")
        exit()
    print("Query created.")
    return query.format(d=d_date,d_id=distributor_id)

def num_to_col(num):
    letters = ''
    x = None
    while num:
        mod = (num-1)%26
        letters +=chr(mod+65)
        num = (num-1)//26
        x = ''.join(reversed(letters))
    return x


def dftoSheetsfast(driver, sheeter, dframe, sp_nam_id=None, sp_nam = None, sh_name='Sheet1',  resize_cols = True, resize_rows = True, append = False, start_row = 1, start_col = 1, folder_id = None, create= False):
    # driver,sheeter = apiconnect()
    dframe.fillna('', inplace=True)
    g = dframe.columns.tolist()
    for i in g:
        if isinstance(dframe[i].iloc[0], dt.datetime) or dframe[i].dtype == 'datetime64[ns]':
            dframe[i] = dframe[i].apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d-%H-%M-%S'))
        elif isinstance(dframe[i].iloc[0], dt.date):
            dframe[i] = dframe[i].apply(lambda x: dt.date.strftime(x, '%Y-%m-%d'))
    keys = dframe.keys()
    x = dframe.values.tolist()
    y = x
    x.insert(0,keys.tolist())
    row, col = dframe.shape
    c = num_to_col(col+start_col-1)
    s_c = num_to_col(start_col)
    r = str(start_row+row)
    sheet_id = sp_nam_id

    body = {
        'values': x
    }

    # media = MediaFileUpload('15_53_Product Variant Rule.csv',
    #                         mimetype='text/csv',
    #                         resumable=True)
    if create:
        file_metadata = {
            'name': sp_nam,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [folder_id]
        }
        sheet = driver.files().create(body=file_metadata, fields='id').execute()
        sheet_id = sheet.get('id')
        addd = {
            "requests":[
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": 0,
                            "title": sh_name
                        },
                        "fields": "title"
                    }
                }
            ],
        }
        sheeter.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=addd).execute()
        dated = sheeter.spreadsheets().values().update(spreadsheetId=sheet_id, body=body, valueInputOption='USER_ENTERED', range=sh_name+'!'+s_c+str(start_row)+':'+c+r).execute()
    elif append:
        body = {
            'values': y
        }
        dated = sheeter.spreadsheets().values().append(spreadsheetId=sheet_id, body=body, valueInputOption='USER_ENTERED', range=sh_name+'!'+s_c+str(start_row)+':'+c+r).execute()
    else:
        dated = sheeter.spreadsheets().values().update(spreadsheetId=sheet_id, body=body, valueInputOption='USER_ENTERED', range=sh_name+'!'+s_c+str(start_row)+':'+c+r).execute()
    sh = sheeter.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_dict = {}
    for i in range(len(sh['sheets'])):
        sheet_dict[sh['sheets'][i]['properties']['title']] = sh['sheets'][i]['properties']
    sh_name_id =sheet_dict[sh_name]['sheetId']
    if resize_cols:
        try:
            req = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sh_name_id,
                                "dimension": "COLUMNS",
                                "startIndex": col

                            }
                        }
                    }
                ],}
            sheeter.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=req).execute()
        except:
            pass
    if resize_rows:
        try:
            req = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sh_name_id,
                                "dimension": "ROWS",
                                "startIndex": row+1

                            }
                        }
                    }
            ],}
            sheeter.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=req).execute()
        except:
            pass

    return sheet_id


def sheetsToDf(sheeter, spreadsheet_id, sh_name):
    output = sheeter.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=sh_name).execute()
    result = pd.DataFrame(output['values'])
    col = (result.iloc[0])
    results = result.drop(0)
    results.columns = col
    # results = results.replace(None, '')
    # print(results.head())
    return results

def sqlToSheetsfast(sql_filename, date, sp_nam_id=None, sp_nam = None, sh_name='Sheet1', driver = None,sheeter = None,  resize_cols = True, resize_rows = True, append = False, start_row = 1, start_col = 1, folder_id = None, create= False):
    if driver == None or driver == None:
        driver,sheeter = apiconnect()
    query = getQuery(sql_filename,date)
    result = fetch4rmDL2(query)
    sheet_id = dftoSheetsfast(driver, sheeter, result, sp_nam_id=sp_nam_id, sp_nam = sp_nam, sh_name=sh_name,  resize_cols = resize_cols, resize_rows = resize_rows, append = append, start_row = start_row, start_col = start_col, folder_id = folder_id, create= create)

def getmdmold():
    return makePGConn('prod-new-mdm-db-replica.ahwspl.net', 5432, 'ganesh_jadhav', '1a0ac387', 'mdm')

def fetch4rmMDM(query):
    conn = getmdmold() 
    print("Connected to MDM. Executing query")
    df = pd.read_sql(query, conn)
    print("Query results fetched.")
    conn.close()
    df.to_csv('test_sample.csv')
    return df

def sqlToSheetsfastMDM(sql_filename, date, sp_nam_id=None, sp_nam = None, sh_name='Sheet1', driver = None,sheeter = None,  resize_cols = True, resize_rows = True, append = False, start_row = 1, start_col = 1, folder_id = None, create= False):
    if driver == None or driver == None:
        driver,sheeter = apiconnect()
    query = getQuery(sql_filename,date)
    result = fetch4rmMDM(query)
    sheet_id = dftoSheetsfast(driver, sheeter, result, sp_nam_id=sp_nam_id, sp_nam = sp_nam, sh_name=sh_name,  resize_cols = resize_cols, resize_rows = resize_rows, append = append, start_row = start_row, start_col = start_col, folder_id = folder_id, create= create)

