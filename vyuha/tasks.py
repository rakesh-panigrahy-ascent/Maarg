from __future__ import absolute_import, unicode_literals

from celery import shared_task

from vyuha.distance_matrix.calculate_distance_matrix_v3 import *
from vyuha.distance_matrix.cluster import *
import vyuha.others.ops_mis_sheeter as oms
from vyuha.connection import *
import json
import sys

app = Celery('tasks', backend='amqp', broker='amqp://')

#@shared_task
@app.task
def add(x, y):
    return x+y

@app.task
def start_distance_matrix_calculation(df, unit_name):
    dist_matrix = distance_matrix()
    result = dist_matrix.distance(df, unit_name)
    return result

@app.task
def calculate_auto_distance_matrix_task(units):
    tableau_data_path = 'vyuha/distance_matrix/input_files/coordinates.csv'
    fetch_tableau_data(output_file = tableau_data_path)
    
    units = json.loads(units)
    units = pd.DataFrame(units)
    
    dist_matrix = distance_matrix()
    for index, row in units.iterrows():
        try:
            state = row['osm_state']
            unit_name = row['dist_name']
            print(unit_name, state)
            temp = units[units['dist_name'] == unit_name]
            temp = temp.loc[:, ['dist_id', 'dist_name', 'latitude', 'longitude']]
            temp['Customer Code'] = temp['dist_name']
            temp['Customer Name'] = temp['dist_name']
            temp.rename(columns={'dist_name':'Distributor Name', 'dist_id':'distributor_id'}, inplace=True)
            df = dist_matrix.calculate_auto_distance_matrix(unit_name, state)
            df = json.loads(df)
            df = pd.DataFrame(df)
            df = pd.concat([df, temp])
            df = df.to_json()
            # if PROD == True:
                # result = start_distance_matrix_calculation.delay(df, unit_name)
            # else:
            result = start_distance_matrix_calculation(df, unit_name)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('Error for: ', state)
            print(exc_type, fname, exc_tb.tb_lineno, str(e))


@app.task
def start_cluster_process(unit_name, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 10):
    cl = Cluster(unit_name, sales_value_benchmark, distance_benchmark, max_clusters)
    print('Clustering Started')
    cl.start()
    cl.export_summary()
    cl.export_reports()

@app.task
def make_mis_final_sheet():
    try:
        oms.main()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Unable to generate mis final sheet !')
        print(exc_type, fname, exc_tb.tb_lineno, str(e))