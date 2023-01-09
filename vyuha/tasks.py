from __future__ import absolute_import, unicode_literals

from celery import shared_task
import vyuha.sheetioQuicks as sq
from vyuha.distance_matrix.calculate_distance_matrix_v3 import *
from vyuha.distance_matrix.cluster import *
import vyuha.others.ops_mis_sheeter as oms
from vyuha.connection import *
import json
import sys

app = Celery('tasks', backend='amqp', broker='amqp://')

driver,sheeter = sq.apiconnect()

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
def start_export_distance_object_to_csv():
    output_dir = 'vyuha/distance_matrix/output_files/objects/'
    output_files = os.listdir(output_dir)
    distributors = [i.split('_')[-1] for i in output_files]
    dist_matrix = distance_matrix()
    for dist in distributors:
        try:
            dist_matrix.export_distance_object_to_csv(dist)
        except Exception as e:
            print('Exception:', str(e))
    print('Objects Exported to CSV Successfully !')

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
            df = dist_matrix.build_engine_data(unit_name, state)
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
def start_cluster_process(unit_name='', sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 30, pincode=[], km_per_hour=20, serving_time=5, time_benchmark=210, auto_mode = 1):

    if auto_mode == 1:
        units = sq.sheetsToDf(sheeter,spreadsheet_id='1omUxQ2wSUgnJxQsdUinp21qfwn-ld8KDSI7Leocrgsg',sh_name='unit-state')
        units = units[units['active'] == '1']
        print(units)
        if units.empty == True:
            resp = {'data':'No Units !'}
            return resp
        units['sales_value_benchmark'] = units['sales_value_benchmark'].astype(int)
        units['distance_benchmark']= units['distance_benchmark'].astype(int)
        units['max_clusters'] = units['max_clusters'].astype(int)
        units['km_per_hour'] = units['km_per_hour'].astype(int)
        units['serving_time'] = units['serving_time'].astype(int)
        units['time_benchmark'] = units['time_benchmark'].astype(int)

        for index, row in units.iterrows():
            sales_value_benchmark = row['sales_value_benchmark']
            distance_benchmark = row['distance_benchmark']
            max_clusters = row['max_clusters']
            km_per_hour = row['km_per_hour']
            serving_time = row['serving_time']
            time_benchmark = row['time_benchmark']
            unit_name = row['dist_name']
            pincode = row['pincode']
            cl = Cluster(hub = unit_name, sales_value_benchmark = sales_value_benchmark, distance_benchmark = distance_benchmark, max_clusters = max_clusters, pincode=[], km_per_hour = km_per_hour, serving_time = serving_time, time_benchmark = time_benchmark)
            print('Clustering Started')
            cl.start()
            cl.export_summary()
            cl.export_reports()
    else:
        cl = Cluster(hub = unit_name, sales_value_benchmark = sales_value_benchmark, distance_benchmark = distance_benchmark, max_clusters = max_clusters, pincode=[], km_per_hour = km_per_hour, serving_time = serving_time, time_benchmark = time_benchmark)
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