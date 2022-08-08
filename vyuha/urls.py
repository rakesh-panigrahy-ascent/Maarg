from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('cluster', views.cluster, name='cluster'),
    path('calculate_distance', views.calculate_distance, name='calculate_distance'),
    path('get_distance_matrix_log', views.get_distance_matrix_log, name='get_distance_matrix_log'),
    path('download_distance_matrix_files', views.download_distance_matrix_files, name='download_distance_matrix_files'),
    path('start_ors', views.start_ors, name='start_ors'),
    path('stop_ors', views.stop_ors, name='stop_ors'),
    path('start_maarg', views.start_maarg, name='start_maarg'),
    path('check_cluster_requirement', views.check_cluster_requirement, name='check_cluster_requirement'),
    path('generate_sales_data', views.generate_sales_data, name='generate_sales_data'),
    path('start_dbscan', views.start_dbscan, name='start_dbscan'),
    path('start_kmeans', views.start_kmeans, name='start_kmeans')
]