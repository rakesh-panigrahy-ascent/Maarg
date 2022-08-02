from asyncio.log import logger
from dataclasses import dataclass
import imp
from optparse import Values
from urllib import response
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from vyuha.distance_matrix.calculate_distance_matrix import *
import logging
from vyuha.tasks import *
from Maarg.settings import *
from vyuha.distance_matrix.config_ors_engine import *
from vyuha.connection import *
from vyuha.distance_matrix.cluster import *

logging.basicConfig(filename='vyuha/distance_matrix/log/distance_matrix.log', level=logging.INFO)

# Create your views here.
def index(request):
   #  return HttpResponse('Welcome To Vyuha Rachana')
   output_dir = 'vyuha/distance_matrix/output_files/'
   output_files = os.listdir(output_dir)
   output_files = [x for x in output_files if '.csv' in x]
   osm_files = get_osm_file_list()
   state_names = [x.split('-')[0].title() for x in osm_files]
   current_osm_file = get_current_osm_file().split('/')[1].split('-')[0].title()

   ors_engine_status = check_ors_status()

   distributors = list(get_distributor_names().loc[:, 'name'])

   return render(request, 'distance_matrix_calculator.html', {'output_files':output_files, 'state_names':state_names, 'current_osm_file':current_osm_file, 'ors_engine_status':ors_engine_status, 'distributors':distributors})


def start_ors(request):
   if PROD == True:
      result = start_container.delay()
   else:
      result = start_container()

   resp = {'data':'ORS Engine Starting...'}
   return JsonResponse(resp)

def stop_ors(request):
   if PROD == True:
      result = stop_container.delay()
   else:
      result = stop_container()
      
   resp = {'data':'Stopping ORS Engine...'}
   return JsonResponse(resp)

def start_clustering(request):
   unit_asset_id = request.POST['unit_asset_id']
   asset_id, unit_name = unit_asset_id.split()[0], ' '.join(unit_asset_id.split()[1:])
   print(asset_id)
   print(unit_name)

   if PROD == True:
      result = start_cluster_process.delay(unit_name, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 10)
   else:
      result = start_cluster_process(unit_name, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 10)
   
   resp = {'data':'Starting Clustering For {}'.format(unit_name)}
   return JsonResponse(resp)

def generate_sales_data(request):
   unit_asset_id = request.POST['unit_asset_id']
   asset_id, unit_name = unit_asset_id.split()[0], ' '.join(unit_asset_id.split()[1:])
   start_date = datetime.today().date() - timedelta(days=31)
   end_date = datetime.today().date() - timedelta(days=1)

   get_cluster_sales_data(str(start_date), str(end_date), asset_id, unit_name)

   resp = {'data':'Starting Clustering...'}
   return JsonResponse(resp) 

def check_cluster_requirement(request):
   unit_asset_id = request.POST['unit_asset_id']
   asset_id, unit_name = unit_asset_id.split()[0], ' '.join(unit_asset_id.split()[1:])

   distance_matrix_file = unit_name.title()+'_distance_matrix.csv'
   output_dir = 'vyuha/distance_matrix/output_files/'
   output_files = os.listdir(output_dir)
   output_files = [x for x in output_files if distance_matrix_file in x]
   if output_files == []:
      distance_matrix = 'Not Available'
   else:
      distance_matrix = output_files[0]

   sales_file = unit_name.title()+'.csv'
   print(sales_file)
   output_dir = 'vyuha/distance_matrix/output_files/sales'
   output_files = os.listdir(output_dir)
   output_files = [x for x in output_files if sales_file in x]
   if output_files == []:
      sales_data = 'Not Available'
   else:
      sales_data = output_files[0]
   # print(sales_data)

   resp = {'distance_matrix':distance_matrix, 'sales_data':sales_data}
   return JsonResponse(resp)

def cluster(request):
   distributors = get_distributor_names_id()
   distributors['key'] = distributors['asset_id'].astype('str') + ' ' + distributors['name']
   # print(distributors)

   return render(request, 'cluster.html', {'distributors':distributors['key']})

def download_distance_matrix_files(request):
   filename = request.GET['filename']
   file_path = 'vyuha/distance_matrix/output_files/'+filename
   if os.path.exists(file_path):
      with open(file_path, 'rb') as fh:
         response = HttpResponse(fh.read(), content_type="application/vnd.ms-excel")
         response['Content-Disposition'] = 'inline; filename=' + os.path.basename(file_path)
         return response
   else:
      return redirect(index)

def calculate_distance(request):
   state = request.POST['state']
   unit_name = request.POST['unitname']
   output_filename = unit_name+'_coordinates.csv'

   current_osm_file = get_current_osm_file().split('/')[1].split('-')[0].title()

   ors_engine_status = check_ors_status()
   
   if ors_engine_status != 'ready':
      resp = {'data': 'ORS Engine Status: {}'.format(ors_engine_status)}
      return JsonResponse(resp)

   if current_osm_file != state:
      change_osm_file(filename=state.lower())

   handle_uploaded_file(request.FILES['file'], output_filename)
   
   input_file = 'vyuha/distance_matrix/input_files/{}'.format(output_filename)
   df = pd.read_csv(input_file)
   df = df.to_json()
   if PROD == True:
      result = start_distance_matrix_calculation.delay(df, unit_name)
   else:
      result = start_distance_matrix_calculation(df, unit_name)
      
   logging.info('Result: '+str(result))
   #dist_matrix = distance_matrix()
   #result = dist_matrix.distance(df, unit_name)

   resp = {'data':'Your task is in queuee...'}

   return JsonResponse(resp)

def handle_uploaded_file(f, filename):
    with open('vyuha/distance_matrix/input_files/{}'.format(filename), 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)

def get_distance_matrix_log(request):
   file_name = 'vyuha/distance_matrix/log/distance_matrix.log'
   file = open(file_name, "r")
   data = ''
   for line in file.readlines():
      data = data+line
   resp = {'data':data}
   return JsonResponse(resp)