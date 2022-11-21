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
        output_file_name += '_distance_matrix.csv'
        distance_df = pd.DataFrame(columns=['from_customer_code','to_customer_code','distance'])
        counter = 0
        for i in range(len(df)):
            for j in range(len(df)):
                orig,dest = list([df['longitude'][i], df['latitude'][i]]), list([df['longitude'][j], df['latitude'][j]])
                distance_in_meter = self.get_distance(orig,dest)
                df2 = {'from_customer_code': df['Customer Code'][i], 'to_customer_code': df['Customer Code'][j], 'distance': distance_in_meter}
                distance_df = distance_df.append(df2, ignore_index=True)
                logging.info(counter)
                counter += 1
        output_file = 'vyuha/distance_matrix/output_files/{}'.format(output_file_name)
        distance_df.to_csv(output_file, index=False)
        logging.info(str(datetime.today())+'-->'+'Done')
        return distance_df
            
    def get_distance(self, origin, destination):
        body = {"locations":[origin,destination], "metrics":["distance"]}
        call = requests.post('http://localhost:8080/ors/v2/matrix/driving-car', json=body, headers=self.headers)
        response_text = json.loads(call.text)
        logging.info(call.status_code)
        if call.status_code == 200:
            distance_in_meter = response_text['distances'][0][1]
        else:
            distance_in_meter = 'NA'
        return distance_in_meter

    def calculate_auto_distance_matrix(self, unit_name, state):
        output_filename = unit_name+'_coordinates.csv'
        current_osm_file = get_current_osm_file().split('/')[1].split('-')[0].title()

        if current_osm_file != state:
            change_osm_file(filename=state.lower())

        ors_engine_status = check_ors_status(1800)
        print('ors_engine_status', ors_engine_status)

        if ors_engine_status != 'ready':
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
        if PROD == True:
            result = start_distance_matrix_calculation.delay(df, unit_name)
        else:
            result = start_distance_matrix_calculation(df, unit_name)
            
        logging.info('Result: '+str(result))
        return 'Task Created !'