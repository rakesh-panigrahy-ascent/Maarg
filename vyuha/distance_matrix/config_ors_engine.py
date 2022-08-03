import os
import json
import subprocess
from time import time
from urllib import request
from Maarg.settings import OSM_CONFIG_JSON_PATH, OSM_DATA_DIR, CONTAINER_ID
import requests
import time

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

def start_container():
    cmd = 'docker container start {}'.format(CONTAINER_ID)
    subprocess.run(cmd)
    return None

def stop_container():
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


def check_ors_status():
    try:
        call = requests.get('http://localhost:8080/ors/v2/health')
        resp =  call.text
        resp = json.loads(resp)
        return resp['status']
    except Exception as e:
        return 'ORS Engine Down! Kindly start and try again !'