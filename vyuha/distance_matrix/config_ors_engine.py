import os
import json
import subprocess

def change_osm_file(filename='odisha'):
    new_file = 'data/{}-latest.osm.pbf'.format(filename)
    
    f_path = "D:/code/ascent/route optimization/Engines/ors/openrouteservice/docker/conf/ors-config.json"
    fin = open(f_path, "r")
    data = fin.read()
    fin.close()

    fout = open(f_path, "w")
    ors_config = json.loads(data)
    ors_config['ors']['services']['routing']['sources'][0] = new_file
    fout.write(json.dumps(ors_config))
    fout.close()

def start_container(container_id='899da61832ec'):
    cmd = 'docker container start {}'.format(container_id)
    subprocess.run(cmd)

def stop_container(container_id='899da61832ec'):
    cmd = 'docker container stop {}'.format(container_id)
    subprocess.run(cmd)