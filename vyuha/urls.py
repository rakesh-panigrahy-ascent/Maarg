from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('hello', views.hello, name='hello'),
    path('hello2', views.say_hello, name='say_hello'),
    path('calculate_distance', views.calculate_distance, name='calculate_distance'),
    path('get_distance_matrix_log', views.get_distance_matrix_log, name='get_distance_matrix_log'),
    path('download_distance_matrix_files', views.download_distance_matrix_files, name='download_distance_matrix_files')
]