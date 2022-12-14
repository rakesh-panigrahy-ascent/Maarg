from asyncio.log import logger
from dataclasses import dataclass
import imp
from optparse import Values
from posixpath import split
from time import process_time_ns
from urllib import response
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse, FileResponse
from vyuha.distance_matrix.calculate_distance_matrix import *
import logging
from vyuha.tasks import *
from Maarg.settings import *
from vyuha.distance_matrix.config_ors_engine import *
from vyuha.connection import *
from vyuha.distance_matrix.cluster import *
import vyuha.sheetioQuicks as sq
from vyuha.others.capacity_planning import *
from dateutil.relativedelta import relativedelta
import zipfile
from vyuha.others.ops_mis_consolidater import *
import vyuha.others.ops_mis_sheeter as oms

driver,sheeter = sq.apiconnect()

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
      result = start_container()
   else:
      result = start_container()

   resp = {'data':'ORS Engine Starting...'}
   return JsonResponse(resp)

def stop_ors(request):
   if PROD == True:
      result = stop_container()
   else:
      result = stop_container()
      
   resp = {'data':'Stopping ORS Engine...'}
   return JsonResponse(resp)

def start_maarg(request):
   unit_name = request.POST['unit']
   auto_mode = 0
   if request.POST['auto_cluster'] == 'true':
      auto_mode = 1

   print(unit_name)

   if PROD == True:
      result = start_cluster_process.delay(unit_name=unit_name, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 30, pincode=[], km_per_hour=20, serving_time=5, time_benchmark=210, auto_mode = auto_mode)
   else:
      result = start_cluster_process(unit_name=unit_name, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 30, pincode=[], km_per_hour=20, serving_time=5, time_benchmark=210, auto_mode = auto_mode)
   
   resp = {'data':'Starting Clustering For {}'.format(unit_name)}
   return JsonResponse(resp)

def start_dbscan(request):
   epsilon = request.POST['epsilon']
   min_samples = request.POST['min_samples']
   unit = request.POST['unit']
   asset_id, unit_name = unit.split()[0], ' '.join(unit.split()[1:])

   distance_matrix_file = unit_name.title()+'_coordinates.csv'

   try:
      handle_uploaded_file(request.FILES['file'], distance_matrix_file)
   except:
      print('No File Uploaded !', str(e))

   input_dir = 'vyuha/distance_matrix/input_files/'
   coordinate_file = input_dir+distance_matrix_file
   try:
      df = pd.read_csv(coordinate_file)
      dbscan = dbscan_cluster(unit_name, df, eps=float(epsilon), min_samples=int(min_samples))
      result = dbscan.start()
      result.rename(columns={'Distributor Name':'distributor_name', 'Customer Code':'customer_code', 'Customer Name':'customer_name'}, inplace=True)

      # exceltodl3(result, 'adhoc.ops_maarg_cluster', conditions={'algo':'dbscan', 'distributor_name':unit_name})
      
      existing_data = sq.sheetsToDf(sheeter,spreadsheet_id=CLUSTER_SHEET_ID,sh_name='Clusters')
      existing_data.drop(existing_data[(existing_data['distributor_name'] == unit_name) & (existing_data['algo'] == 'dbscan')].index, inplace=True)
      result = pd.concat([result, existing_data])
      sq.dftoSheetsfast(driver,sheeter,result,sp_nam_id=CLUSTER_SHEET_ID,sh_name='Clusters')

      resp = {'data':'Clusters Created !'}
   except Exception as e:
      print(str(e))
      resp = {'data':str(e)}

   return JsonResponse(resp)

def start_kmeans(request):
   kmeans_n_clusters = int(request.POST['kmeans_n_clusters'])
   kmeans_n_init = int(request.POST['kmeans_n_init'])
   kmeans_max_iter = int(request.POST['kmeans_max_iter'])
   kmeans_tol = float(request.POST['kmeans_tol'])
   kmeans_algorithm = request.POST['kmeans_algorithm']

   unit = request.POST['unit']
   asset_id, unit_name = unit.split()[0], ' '.join(unit.split()[1:])
   print(unit_name)
   distance_matrix_file = unit_name.title()+'_coordinates.csv'
   
   try:
      handle_uploaded_file(request.FILES['file'], distance_matrix_file)
   except Exception as e:
      print('No File Uploaded !', str(e))

   input_dir = 'vyuha/distance_matrix/input_files/'
   coordinate_file = input_dir+distance_matrix_file
   try:
      df = pd.read_csv(coordinate_file)
      kmeans = kmeans_cluster(unit_name, df, n_clusters=kmeans_n_clusters, n_init=kmeans_n_init, max_iter=kmeans_max_iter, tol=kmeans_tol, algorithm=kmeans_algorithm)
      result = kmeans.start()
      result.rename(columns={'Distributor Name':'distributor_name', 'Customer Code':'customer_code', 'Customer Name':'customer_name'}, inplace=True)
      # exceltodl3(result, 'adhoc.ops_maarg_cluster', conditions={'algo':'kmeans', 'distributor_name':unit_name})
      
      existing_data = sq.sheetsToDf(sheeter,spreadsheet_id=CLUSTER_SHEET_ID,sh_name='Clusters')
      existing_data.drop(existing_data[(existing_data['distributor_name'] == unit_name) & (existing_data['algo'] == 'kmeans')].index, inplace=True)
      result = pd.concat([result, existing_data])

      sq.dftoSheetsfast(driver,sheeter,result,sp_nam_id=CLUSTER_SHEET_ID,sh_name='Clusters')
      
      resp = {'data':'Clusters Created !'}
   except Exception as e:
      print(str(e))
      resp = {'data':str(e)}

   return JsonResponse(resp)

def generate_sales_data(request):
   unit_asset_id = request.POST['unit_asset_id']
   asset_id, unit_name = unit_asset_id.split()[0], ' '.join(unit_asset_id.split()[1:])

   previous_month_date = datetime.today() - relativedelta(months=1)
   start_date = current_date = datetime(previous_month_date.year, previous_month_date.month, 1).date()
   end_date = start_date + relativedelta(months=1) - timedelta(days=1)

   get_cluster_sales_data(str(start_date), str(end_date), asset_id, unit_name)

   resp = {'data':'Sales Data Generated !'}
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

   sales_file = 'sales.csv'#unit_name.title()+'.csv'

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

def run_distance_matrix_cron():
   units = sq.sheetsToDf(sheeter,spreadsheet_id='1omUxQ2wSUgnJxQsdUinp21qfwn-ld8KDSI7Leocrgsg',sh_name='unit-state')
   units = units[units['active'] == '1']
   units['latitude'] = units['latitude'].astype(float)
   units['longitude'] = units['longitude'].astype(float)
   print(units)
   print(type(units))
   if units.empty == True:
      resp = {'data':'No Units !'}
      return JsonResponse(resp)
   units = units.to_json()
   if PROD == True:
      result = calculate_auto_distance_matrix_task(units)
   else:
      result = calculate_auto_distance_matrix_task(units)

   resp = {'data':'Your task is in queuee...'}
   return JsonResponse(resp)

def calculate_distance(request):
   auto_distance_matrix = request.POST['auto_distance_matrix']
   if auto_distance_matrix == 'true':
      return run_distance_matrix_cron()

   state = request.POST['state']
   unit_name = request.POST['unitname']
   output_filename = unit_name+'_coordinates.csv'

   current_osm_file = get_current_osm_file().split('/')[1].split('-')[0].title()

   if current_osm_file != state:
      change_osm_file(filename=state.lower())

   ors_engine_status = check_ors_status(120)
   print('ors_engine_status', ors_engine_status)
   if ors_engine_status != 'ready':
      resp = {'data': 'ORS Engine Status: {}'.format(ors_engine_status)}
      return JsonResponse(resp)
   
   tableau_data_path = 'vyuha/distance_matrix/input_files/coordinates.csv'
   output_file_path = 'vyuha/distance_matrix/input_files/'+output_filename
   fetch_tableau_data(output_file = tableau_data_path)
   coordinate_file = pd.read_csv(tableau_data_path)
   coordinate_file.rename(columns={'Median latitude':'latitude', 'Median longitude':'longitude'}, inplace=True)
   coordinate_file = coordinate_file[coordinate_file['Distributor Name'] == unit_name]
   coordinate_file = coordinate_file.loc[:, ['distributor_id', 'Distributor Name', 'Customer Code', 'Customer Name', 'latitude', 'longitude']]
   print(coordinate_file)
   if os.path.exists('vyuha/distance_matrix/input_files/') == False:
      os.makedirs('vyuha/distance_matrix/input_files/')
   coordinate_file.to_csv(output_file_path, index=False)

   # handle_uploaded_file(request.FILES['file'], output_filename)
   
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

def handle_uploaded_file(f, filename, path='vyuha/distance_matrix/input_files/'):
    with open('{}{}'.format(path, filename), 'wb+') as destination:
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

def capacity_planning(request):
   day_hours = np.arange(0, 24)
   # output_dir = 'vyuha/others/files/output_files/'
   # weekday_summary_path = output_dir + 'Capacity Planning Weekday Summary.csv'
   # weekday_summary = pd.read_csv(weekday_summary_path)
   # print(weekday_summary.to_dict())
   output_dir = 'vyuha/others/files/output_files/'
   output_files = os.listdir(output_dir)
   output_files = sorted([x for x in output_files if '.csv' in x])

   resp = {'day_hours':day_hours, 'output_files':output_files}
   return render(request, 'capacity_planning.html', resp)

def calculate_capacity(request):
   start_date = datetime.strptime(request.POST['start_date'], '%Y-%m-%d').date()
   end_date = datetime.strptime(request.POST['end_date'], '%Y-%m-%d').date()
   start_hour = 0#int(request.POST['start_hour'])
   end_hour = 23#int(request.POST['end_hour'])
   # kpi = request.POST['kpi']
   picker_capacity = int(request.POST['picker_capacity'])
   checker_capacity = int(request.POST['checker_capacity'])
   dispatch_capacity = int(request.POST['dispatch_capacity'])
   total_projection_value = int(request.POST['total_projection_value'])
   v1_value = int(request.POST['v1_value'])
   v2_value = int(request.POST['v2_value'])
   roaster = request.POST['roaster']
   
   serviceability = float(request.POST['serviceability'])
   split_hour = int(request.POST['split_hour'])
   roaster_result = pd.DataFrame()
   for kpi in ['Picking', 'Checking', 'Dispatch']:
      if kpi == 'Picking':
         kpi_name = 'total_line_items'
         capacity = picker_capacity #70
      elif kpi == 'Checking':
         kpi_name = 'quantity'
         capacity = checker_capacity #450
      elif kpi == 'Dispatch':
         kpi_name = 'challan_count'
         capacity = dispatch_capacity#59
      else:
         kpi_name = 'modified_line_items'
         capacity = 70

      input_dir = 'vyuha/others/files/input_files/'
      
      input_file = input_dir+'Capacity Planning.csv'
      df = pd.read_csv(input_file)
      print(df)
      current_date = start_date.replace(day=1)
      end_date = end_date + relativedelta(months=1)
      end_date = end_date.replace(day=1) - timedelta(days=1)

      if total_projection_value != 0:
         col = df.columns
         revenue_Data = sq.sheetsToDf(sheeter,spreadsheet_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='revenue data')
         
         df['scan_date'] = pd.to_datetime(df['scan_date'])
         revenue_Data['Date'] = pd.to_datetime(revenue_Data['Date'])
         
         df['yr'] = df['scan_date'].dt.year
         df['month'] = df['scan_date'].dt.month
         revenue_Data['yr'] = revenue_Data['Date'].dt.year
         revenue_Data['month'] = revenue_Data['Date'].dt.month

         final_data = df.merge(revenue_Data, on=['dist_name', 'month', 'yr'], how='left')
         final_data['Total'] = final_data['Total'].astype(float)
         final_data['V2'] = final_data['V2'].astype(float)
         final_data['V1'] = final_data['V1'].astype(float)
         final_data['challan_count'] = (final_data['challan_count'] / final_data['V2'])*v2_value
         final_data['total_line_items'] = (final_data['total_line_items'] / final_data['V2'])*v2_value
         final_data['item_value'] = (final_data['item_value'] / final_data['V2'])*v2_value
         final_data['quantity'] = (final_data['quantity'] / final_data['V2'])*v2_value
         print(final_data)
         final_data = final_data[col]
      else:
         final_data = df.copy()

      while current_date <= end_date:
         last_date = current_date + relativedelta(months=1) - timedelta(days=1)
         print(kpi_name)
         print(current_date, last_date)
         cp = CapacityPlanning(final_data, current_date, last_date,  capacity, start_hour, end_hour, kpi_name, split_hour, kpi, serviceability, total_projection_value, v1_value, v2_value, roaster)
         cp.start()
         current_date += relativedelta(months=1)
   if roaster == 'true':
      roaster = Roaster()
      roaster.start()
   else:
      cp.fetch_tableau_data()
      cp.export_final_data(start_date, end_date)
   

   

   resp = {'data':'Calculation done!'}
   return JsonResponse(resp)


def download_result(request):
   filename = request.GET['download_result']
   file_path = 'vyuha/others/files/output_files/'+filename
   
   if os.path.exists(file_path):
      file = open(file_path, 'rb')
      response = FileResponse(file, content_type='text/csv')
      response['Content-Disposition'] = 'attachment; filename="'+filename+'"'
      return response
   else:
      print('Not Exists!')
      return redirect(capacity_planning)

def get_capacity_data(request):
   start_date = datetime.strptime(request.POST['start_date'], '%Y-%m-%d').date()
   end_date = datetime.strptime(request.POST['end_date'], '%Y-%m-%d').date()
   
   start_date = start_date.replace(day=1)
   end_date = end_date + relativedelta(months=1)
   end_date = end_date.replace(day=1)
   end_date -= timedelta(days=1)
   
   print(start_date)
   print(end_date)

   query = getQuery('capacity_planning.sql', start_date, end_date)
   t = text(query)
   print(t)
   conn = get_dl3_connection()
   print('Fetching Data...')
   df = pd.read_sql(t, conn)
   input_dir = 'vyuha/others/files/input_files/'
   input_file = input_dir+'Capacity Planning.csv'
   if os.path.exists('vyuha/others/files/input_files/') == False:
      os.makedirs('vyuha/others/files/input_files/')
   df.to_csv(input_file, index=False)
   resp = {'data':'Imported Data!'}
   return JsonResponse(resp)


def ops_mis(request):
   output_dir = 'vyuha/others/mis/output_files/'
   output_files = os.listdir(output_dir)
   output_files = [x for x in output_files if '.csv' in x]
   return render(request, 'ops_mis.html', {'output_files':output_files})

def build_mis(request):
   
   miszip = 'MIS.zip'
   path = 'vyuha/others/mis/input_files/'
   path_to_zip_file = path+miszip
   try:
      handle_uploaded_file(request.FILES['file'], miszip, path=path)
   except Exception as e:
      print('No File Uploaded !', str(e))

   
   with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
      zip_ref.extractall(path)
   
   #Generate MIS Key File
   try:
      opsmis = OpsMIS()
      opsmis.start_pipeline()
   except Exception as e:
      exc_type, exc_obj, exc_tb = sys.exc_info()
      fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
      print('Unable to generate mis key file !')
      print(exc_type, fname, exc_tb.tb_lineno, str(e))
      resp = {'data':'Unable to generate key file !...'}
      return JsonResponse(resp)

   #Generate MIS Final Sheet
   try:
      if PROD == True:
         result = make_mis_final_sheet.delay()
      else:
         result = make_mis_final_sheet()
   except Exception as e:
      exc_type, exc_obj, exc_tb = sys.exc_info()
      fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
      print('Unable to generate mis final sheet !')
      print(exc_type, fname, exc_tb.tb_lineno, str(e))


   

   resp = {'data':'Building in progress...'}
   return JsonResponse(resp)