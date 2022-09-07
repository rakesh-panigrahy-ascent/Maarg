import pandas as pd
import numpy as np
import warnings
import requests
import json
import os
import logging
from datetime import datetime
from celery import Celery
import sys
from itertools import product


warnings.filterwarnings("ignore")

logging.basicConfig(filename='vyuha/distance_matrix/log/distance_matrix.log', level=logging.INFO)

celery = Celery('distance_matrix_calculate', backend='redis://localhost/0', broker='redis://localhost/0')  

class distance_matrix:

    def __init__(self):
        self.headers = {
            'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
            'Authorization': '5b3ce3597851110001cf62488808957a219b4d4abf24063f0fa5b1a2',
            'Content-Type': 'application/json; charset=utf-8'
        }
    

    def distance(self,df, output_file_name):
        df = json.loads(df)
        df = pd.DataFrame(df)
        logging.info(str(datetime.today()))

        x = df.loc[:, ['longitude', 'latitude']].values
        x = x.tolist()

        master_list = []
        for n in range(len(x)-9):
            print('n', n)
            for i in range(n+1, len(x), 9):
                try:
                    last_index = i+9
                    if i+9 > len(x)+1:
                        last_index = len(x)+1
                    print(n, i, last_index)
                    coords = x[i:last_index]
                    coords.append(x[n])
                    print(coords)
                    body = {"locations":coords, "metrics":["distance"]}
                    call = requests.post('http://localhost:8080/ors/v2/matrix/driving-car', json=body, headers=self.headers)
                    print(call.status_code, call.reason)
                    master_list.append(call.text)
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno, str(e))
        

        distance_matrix_df = pd.DataFrame(columns=['source', 'destination', 'distance'])
        source = []
        dest = []
        distance = []
        for data in master_list:
            x = json.loads(data)
            try:
                for i in range(len(x['distances'][0])):
                    json.loads(master_list[0])['metadata']['query']['locations']
                    for j in range(len(x['distances'][0])):
            #             print(x['metadata']['query']['locations'][i], x['metadata']['query']['locations'][j], x['distances'][i][j])
                        source.append(x['metadata']['query']['locations'][i])
                        dest.append(x['metadata']['query']['locations'][j])
                        distance.append(x['distances'][i][j])
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno, str(e))

        distance_matrix_df['source'] = source
        distance_matrix_df['destination'] = dest
        distance_matrix_df['distance'] = distance

        distance_matrix_df['s_lon'] = distance_matrix_df['source'].apply(lambda x:x[0])
        distance_matrix_df['s_lat'] = distance_matrix_df['source'].apply(lambda x:x[1])
        distance_matrix_df['d_lon'] = distance_matrix_df['destination'].apply(lambda x:x[0])
        distance_matrix_df['d_lat'] = distance_matrix_df['destination'].apply(lambda x:x[1])
        distance_matrix_df.drop(columns=['source', 'destination'], inplace=True)
        distance_matrix_df.drop_duplicates(inplace=True)


        dt_f = pd.DataFrame()
        customers = list(product(df['Customer Code'], df['Customer Code']))
        dt_f['customers'] = customers
        dt_f['from_customer_code'] = dt_f['customers'].apply(lambda x:x[0])
        dt_f['to_customer_code'] = dt_f['customers'].apply(lambda x:x[1])

        dt_f = dt_f.merge(df, left_on=['from_customer_code'], right_on=['Customer Code'], how='left')
        dt_f.drop(columns=['distributor_id', 'Distributor Name', 'Customer Code','Customer Name', 'customers'], inplace=True)
        dt_f.rename(columns={'latitude':'s_lat', 'longitude':'s_lon'}, inplace=True)

        dt_f = dt_f.merge(df, left_on=['to_customer_code'], right_on=['Customer Code'], how='left')
        dt_f.drop(columns=['distributor_id', 'Distributor Name', 'Customer Code','Customer Name'], inplace=True)
        dt_f.rename(columns={'latitude':'d_lat', 'longitude':'d_lon'}, inplace=True)

        final_df = dt_f.merge(distance_matrix_df, on=['s_lat', 's_lon', 'd_lat', 'd_lon'], how='left')

        # final_df.drop(columns=['d_lat', 'd_lon', 's_lat','s_lon'], inplace=True)
        # final_df.drop_duplicates(subset=['from_customer_code', 'to_customer_code'], inplace=True)

        output_file_name += '_distance_matrix.csv'
        output_file = 'vyuha/distance_matrix/output_files/{}'.format(output_file_name)
        
        final_df.to_csv(output_file, index=False)
        logging.info(str(datetime.today())+'-->'+'Done')
        return final_df