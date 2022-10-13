import os
import json
import subprocess
from time import sleep, time
from urllib import request
from Maarg.settings import OSM_CONFIG_JSON_PATH, OSM_DATA_DIR, CONTAINER_ID
import requests
import time
import logging
logging.basicConfig(filename='vyuha/distance_matrix/log/distance_matrix.log', level=logging.INFO)

def change_osm_file(filename='odisha'):
    new_file = 'data/{}-latest.osm.pbf'.format(filename)
    
    f_path = OSM_CONFIG_JSON_PATH
    fin = open(f_path, "r")
    data = fin.read()
    fin.close()

    fout = open(f_path, "w")
    ors_config = json.loads(data)
    ors_config['ors']['services']['routing']['sources'][0] = new_file
    fout.write(json.dumps(ors_config))
    fout.close()
    stop_container()
    start_container()

def start_container():
    logging.info('Starting Container')
    cmd = 'docker container start {}'.format(CONTAINER_ID)
    subprocess.run(cmd)
    return None

def stop_container():
    logging.info('Stoping Container')
    cmd = 'docker container stop {}'.format(CONTAINER_ID)
    subprocess.run(cmd)
    return None

def get_osm_file_list():
    f_path = OSM_DATA_DIR
    files = os.listdir(f_path)
    files = [x for x in files if 'pbf' in x] 
    return files

def get_current_osm_file():
    f_path = OSM_CONFIG_JSON_PATH
    fin = open(f_path, "r")
    data = fin.read()
    fin.close()
    ors_config = json.loads(data)
    osm_file = ors_config['ors']['services']['routing']['sources'][0]
    return osm_file


def check_ors_status(timer=5):
    start_time = time()
    end_time = time()
    duration = end_time - start_time
    while duration <= timer:
        print('Duration: {} seconds', duration)
        try:
            call = requests.get('http://localhost:8080/ors/v2/health', timeout=10)
            resp =  call.text
            resp = json.loads(resp)
            status = resp['status']
            if status == 'ready':
                break
        except Exception as e:
            status = 'ORS Engine Down! Kindly start and try again !'
        end_time = time.time()
        duration = end_time - start_time
        sleep(5)
    return status