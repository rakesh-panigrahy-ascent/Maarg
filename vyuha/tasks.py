from __future__ import absolute_import, unicode_literals

from celery import shared_task

from vyuha.distance_matrix.calculate_distance_matrix_v3 import *
from vyuha.distance_matrix.cluster import *
import vyuha.others.ops_mis_sheeter as oms
from vyuha.connection import *

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
    
    dist_matrix = distance_matrix()
    for index, row in units.iterrows():
        state = row['osm_state']
        unit_name = row['dist_name']
        print(unit_name, state)
        df = dist_matrix.calculate_auto_distance_matrix(unit_name, state)
        # if PROD == True:
            # result = start_distance_matrix_calculation.delay(df, unit_name)
        # else:
        result = start_distance_matrix_calculation(df, unit_name)

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