import pandas as pd
import numpy as np
import math
import os
from datetime import datetime, timedelta
from vyuha.distance_matrix.cluster_validate import *


class Cluster:
    def __init__(self, hub, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 10):
        sales_path = 'vyuha/distance_matrix/output_files/sales/{}.csv'.format(hub)
        distance_matrix_path = 'vyuha/distance_matrix/output_files/{}_distance_matrix.csv'.format(hub)
        self.distance_matrix = pd.read_csv(distance_matrix_path)
        self.sales = pd.read_csv(sales_path)
        print(self.sales)
        print(self.distance_matrix)
        
        self.hub = hub
        self.counter = 0
        
        self.sales_value_benchmark = sales_value_benchmark
        self.distance_benchmark = distance_benchmark
        self.max_clusters = max_clusters
        self.vl = Validate(hub, sales_value_benchmark, distance_benchmark)
        
        
        self.distance_matrix['to_customer_code'] = self.distance_matrix['to_customer_code'].astype('str')
        self.distance_matrix['from_customer_code'] = self.distance_matrix['from_customer_code'].astype('str')
        
        self.distance_matrix.drop(self.distance_matrix[(self.distance_matrix['to_customer_code'] == str(hub))].index, inplace=True)
        self.distance_matrix.drop(self.distance_matrix[(self.distance_matrix['to_customer_code'] == self.distance_matrix['from_customer_code'])].index, inplace=True)
        
        
        self.total_customers = len(set(self.distance_matrix.from_customer_code))
        
        self.places = []
        self.distances = []
        self.desc = []
        self.summary = []
        
        self.distance_dict = {}
        self.places_dict = {}
        self.desc_dict = {}
        self.summary_dict = {}
        
        self.counter = 0
        self.main_counter = 0
        self.customers_clustered = []
        # print(self.sales)
    def make_cluster(self, distance_matrix, current_place=None):
        if current_place == None:
            current_place = self.hub

        print('Cluster: ',self.counter)
        print('Clustering For: ', len(self.customers_clustered))
        if len(self.customers_clustered) >= self.total_customers+1 or self.counter > self.max_clusters or distance_matrix.shape[0] <= self.total_customers or distance_matrix.empty:
            self.reset()
            print('Exit')
            return 'Done'
        else:
            if current_place == self.hub:
                self.places.append(current_place)
                self.distances.append(0)
                
            result = self.next_nearest_customer(distance_matrix, str(current_place))
            print(result['from_customer_code'], '-->' ,result['to_customer_code'], '-->', result['distance'])
            
            self.places.append(result['to_customer_code'])
            self.distances.append(result['distance'])
            
            # Check Start
            status = self.check(self.places)
            if status == False:
                self.reset()
                current_place = self.hub
            else:
                current_place = str(result['to_customer_code'])
                self.customers_clustered.append(result['to_customer_code'])
                if result['from_customer_code'] != self.hub:
                    distance_matrix.drop(distance_matrix[(distance_matrix['to_customer_code'] == str(result['from_customer_code']))].index, inplace=True)
            
#             if len(self.places) == 5:
#                 self.reset()
            
            # Check Stop
            
            self.main_counter += 1
#             if self.main_counter == 10:
#                 distance_matrix = pd.DataFrame()
            self.make_cluster(distance_matrix, current_place)
    
    def check(self, customers):
        #Validate
        print(customers)
        desc = self.vl.validate(customers)
        summary = self.vl.summary()
        print(summary)
        
#         if summary['SALE VALUE'] >= 90:
#         if summary['DAILY SALE VALUE'] <= self.sales_value_benchmark:
        if summary['DAILY TRAVEL DISTANCE'] <= self.distance_benchmark:
            self.summary = summary
            self.desc = desc
            return True
        else:
            return False
        
    
    def reset(self):
        self.places_dict[self.counter] = self.places
        self.distance_dict[self.counter] = self.distances
        self.desc_dict[self.counter] = self.desc
        self.summary_dict[self.counter] = self.summary


        cluster_report_path = 'vyuha/distance_matrix/output_files/cluster_reports/{}'.format(self.hub)
        if os.path.isdir(cluster_report_path) == False:
            os.makedirs(cluster_report_path)
        cluster_report_path += '/Cluster {} Report.csv'.format(self.counter)

        self.desc.to_csv(cluster_report_path)
        
        
        self.counter += 1
        self.places = []
        self.distances = []
        self.desc = []
        self.summary = []
    
    def next_nearest_customer(self, distance_matrix, current_customer):
        print(current_customer.strip())
        self.latest_distance_matrix = distance_matrix
        result = distance_matrix[distance_matrix['from_customer_code'] == current_customer].sort_values('distance').iloc[0,:]
        return result
    
    def start(self):
        print('Engine running...')
        self.make_cluster(self.distance_matrix)
        
    def export_reports(self):
        final_output = pd.DataFrame(columns=['Cluster', 'Customer'])
        cluster = []
        places = []
        for x in range(len(self.places_dict)):
            for place in self.places_dict[x]:
                places.append(place)
                cluster.append(x)
        
        cluster_report_path = 'vyuha/distance_matrix/output_files/cluster_reports/{}'.format(self.hub)
        if os.path.isdir(cluster_report_path) == False:
            os.makedirs(cluster_report_path)
        cluster_report_path += '/cluster_report.csv'

        final_output.to_csv(cluster_report_path, index=True)
        
    def export_summary(self):
        summary_path = 'vyuha/distance_matrix/output_files/summary/{}_summary.txt'.format(self.hub)
        file = open(summary_path, 'w+')
        for i in self.summary_dict:
            sale_value_pass_rate = self.summary_dict[i]['SALE VALUE']
            distance_value_pass_rate = self.summary_dict[i]['DISTANCE']
            avg_daily_sale_value = self.summary_dict[i]['DAILY SALE VALUE']
            avg_daily_distance = self.summary_dict[i]['DAILY TRAVEL DISTANCE']

            sentence = 'Cluster: {}\nSale Value Pass: {}%\nDistance Pass: {}%\nAvg Daily Sale: {}\nAvg Daily Distance: {}\n\n'.format(i, sale_value_pass_rate, distance_value_pass_rate, avg_daily_sale_value, avg_daily_distance)
            file.write(sentence)
        file.close()