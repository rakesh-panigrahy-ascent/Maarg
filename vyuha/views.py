from asyncio.log import logger
from dataclasses import dataclass
from urllib import response
from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from vyuha.distance_matrix.calculate_distance_matrix import *
import logging

logging.basicConfig(filename='vyuha/distance_matrix/log/distance_matrix.log', level=logging.INFO)

# Create your views here.
def index(request):
   #  return HttpResponse('Welcome To Vyuha Rachana')
   output_dir = 'vyuha/distance_matrix/output_files/'
   output_files = os.listdir(output_dir)
   return render(request, 'distance_matrix_calculator.html', {'output_files':output_files})

def hello(request):
   text = """<h1>welcome to my app !</h1>"""
   return HttpResponse(text)

def say_hello(request):
   
   return render(request, 'hello.html', {'name':'Mosh'})

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

   handle_uploaded_file(request.FILES['file'], output_filename)
   
   input_file = 'vyuha/distance_matrix/input_files/{}'.format(output_filename)
   df = pd.read_csv(input_file)

   dist_matrix = distance_matrix()
   result = dist_matrix.distance(df, unit_name)

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