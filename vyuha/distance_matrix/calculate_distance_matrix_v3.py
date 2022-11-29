import pandas as pd
import numpy as np
import warnings
import requests
import json
import os
import logging
from datetime import datetime
from celery import Celery
from vyuha.distance_matrix.config_ors_engine import *
from vyuha.tasks import *
import sys
from itertools import product
import re
from vyuha.mailer.common_mailer import send_mail

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

    def call_distance_matrix_api(self, coords):
        if len(coords) == 0:
            return False
        
        body = {"locations":coords, "metrics":["distance"]}
        call = requests.post('http://localhost:8080/ors/v2/matrix/driving-car', json=body, headers=self.headers)
        print(call.status_code, call.reason)
        
        if call.status_code == 404:
            resp = json.loads(call.text)
            error_msg = resp['error']['message']
            print(error_msg)
            
            try:
                txt = "bounds: [0-9]+\.[0-9]+,+[0-9]+\.[0-9]+. Destination"
                error_coord = re.findall(txt, error_msg)[0]
            except:
                txt = "bounds: [0-9]+\.[0-9]+,+[0-9]+\.[0-9]+."
                error_coord = re.findall(txt, error_msg)[0]
                
            txt = "[0-9]+\.[0-9]+,+[0-9]+\.[0-9]+"
            error_coord = re.findall(txt, error_coord)
            error_coord = [[i] for i in error_coord]
            error_coord = [j[0].split(',') for j in error_coord]
            print(error_coord)

            c = [float(error_coord[0][1]), float(error_coord[0][0])]
            print('To be Excluded', c)
            coords.remove(c)
            self.call_distance_matrix_api(coords)
        return call

    
    

    def distance(self,df, output_file_name):
        print('Distance Calculation Started for ', output_file_name)
        df = json.loads(df)
        df = pd.DataFrame(df)
        logging.info(str(datetime.today()))
        print('Dataframe:', df.head())
        
        x = df.loc[:, ['longitude', 'latitude']].values
        x = x.tolist()
        for c in x:
            if c == [0.0, 0.0]:
                x.remove(c)
        

        master_list = []
        if len(x) > 10:
            for n in range(len(x)-9):
                print('n', n)
                for i in range(n+1, len(x), 9):
                    last_index = i+9
                    if i+9 > len(x)+1:
                        last_index = len(x)+1
                    print(n, i, last_index)
                    coords = x[i:last_index]
                    coords.append(x[n])
                    print(coords)
                    call = self.call_distance_matrix_api(coords)
                    if call == False:
                        continue
                    master_list.append(call.text)
        else:
            coords = x
            print(coords)
            call = self.call_distance_matrix_api(coords)
            if call != False:
                master_list.append(call.text)

        

        source = []
        dest = []
        distance = []
        distance_matrix_df = pd.DataFrame(columns=['source', 'destination', 'distance'])
        for data in master_list:
            try:
                x = json.loads(data)
                locations = x['metadata']['query']['locations']
                distances = x['distances']
                for i in range(len(locations)):
                    for j in range(len(locations)):
                        source.append(locations[i])
                        dest.append(locations[j])
                        distance.append(distances[i][j])
            except Exception as e:
                print(data)
        distance_matrix_df['source'] = source
        distance_matrix_df['destination'] = dest
        distance_matrix_df['distance'] = distance

        distance_matrix_df['s_lon'] = distance_matrix_df['source'].apply(lambda x:x[0])
        distance_matrix_df['s_lat'] = distance_matrix_df['source'].apply(lambda x:x[1])
        distance_matrix_df['d_lon'] = distance_matrix_df['destination'].apply(lambda x:x[0])
        distance_matrix_df['d_lat'] = distance_matrix_df['destination'].apply(lambda x:x[1])
        distance_matrix_df.drop(columns=['source', 'destination'], inplace=True)
        distance_matrix_df.drop_duplicates(inplace=True)
        print('Shape:', distance_matrix_df.shape)
        
        try:
            distance_matrix_df_cp = distance_matrix_df.copy()
            df_c = df.copy()
            df_c.rename(columns={'latitude':'s_lat', 'longitude':'s_lon'}, inplace=True)
            distance_matrix_df_cp = distance_matrix_df_cp.merge(df_c, on=['s_lat','s_lon'], how='left')
            distance_matrix_df_cp.rename(columns={'Customer Code':'from_customer_code'}, inplace=True)
            df_c = df.copy()
            df_c.rename(columns={'latitude':'d_lat', 'longitude':'d_lon'}, inplace=True)
            distance_matrix_df_cp = distance_matrix_df_cp.merge(df_c, on=['d_lat','d_lon'], how='left')
            distance_matrix_df_cp.rename(columns={'Customer Code':'to_customer_code'}, inplace=True)
            distance_matrix_df_cp.drop(columns=['distributor_id_x', 'Distributor Name_x', 'Customer Name_x', 'Customer Name_y', 'distributor_id_y', 'Distributor Name_y'], inplace=True)
            # distance_matrix_df_cp
            output_file_name += '_distance_matrix.csv'
            output_file = 'vyuha/distance_matrix/output_files/{}'.format(output_file_name)
            distance_matrix_df_cp.to_csv(output_file, index=False)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('Error for: ')
            print(exc_type, fname, exc_tb.tb_lineno, str(e))

        logging.info(str(datetime.today())+'-->'+'Done')
        return distance_matrix_df_cp

    def calculate_auto_distance_matrix(self, unit_name, state):
        output_filename = unit_name+'_coordinates.csv'
        current_osm_file = get_current_osm_file().split('/')[1].split('-')[0].title()

        print('Current OSM: ', current_osm_file)
        print('State: ', state)
        if current_osm_file.lower() != state.lower():
            change_osm_file(filename=state.lower())

        ors_engine_status = check_ors_status(1800)
        print('ors_engine_status', ors_engine_status)

        if ors_engine_status != 'ready':
            subject = 'Engine Starting Failed'
            to = ['rakesh.panigrahy@ahwspl.com'] 
            cc = []
            text = 'Not started for {}'.format(state)
            send_mail(subject, to, cc, text)
            resp = {'data': 'ORS Engine Status: {}'.format(ors_engine_status)}
            return resp
        
        tableau_data_path = 'vyuha/distance_matrix/input_files/coordinates.csv'
        output_file_path = 'vyuha/distance_matrix/input_files/'+output_filename
        coordinate_file = pd.read_csv(tableau_data_path)
        coordinate_file.rename(columns={'Median latitude':'latitude', 'Median longitude':'longitude'}, inplace=True)
        coordinate_file = coordinate_file[coordinate_file['Distributor Name'] == unit_name]
        coordinate_file = coordinate_file.loc[:, ['distributor_id', 'Distributor Name', 'Customer Code', 'Customer Name', 'latitude', 'longitude']]
        print(coordinate_file)
        coordinate_file.to_csv(output_file_path, index=False)
        
        df = coordinate_file.to_json()
        return df