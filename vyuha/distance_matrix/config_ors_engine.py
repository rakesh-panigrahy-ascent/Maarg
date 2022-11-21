import time
import os
import json
import sys
import subprocess
from time import sleep, time
from urllib import request
from Maarg.settings import OSM_CONFIG_JSON_PATH, OSM_DATA_DIR, CONTAINER_ID, PROD
import requests
import time
import docker
import logging
logging.basicConfig(filename='vyuha/distance_matrix/log/distance_matrix.log', level=logging.INFO)

try:
    client = docker.from_env()
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]  
    print(exc_type, fname, exc_tb.tb_lineno, str(e))



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
    restart_container()

def start_container():
    logging.info('Starting Container')
    if PROD == False:
        cmd = 'docker container start {}'.format(CONTAINER_ID)
        subprocess.run(cmd)
    else:
        container = client.containers.get(CONTAINER_ID)
        container.start()
    return None

def stop_container():
    logging.info('Stoping Container')
    if PROD == False:
        cmd = 'docker container stop {}'.format(CONTAINER_ID)
        subprocess.run(cmd)
    else:
        container = client.containers.get(CONTAINER_ID)
        container.stop()
    return None

def restart_container():
    logging.info('Restarting Container')
    if PROD == False:
        cmd = 'docker container restart {}'.format(CONTAINER_ID)
        subprocess.run(cmd)
    else:
        container = client.containers.get(CONTAINER_ID)
        container.restart()
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
    start_time = time.time()
    end_time = time.time()
    duration = end_time - start_time
    while duration <= timer:
        print('Duration: {} seconds'.format(duration))
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
        time.sleep(5)
    return status