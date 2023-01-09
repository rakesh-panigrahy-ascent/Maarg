import pandas as pd
import numpy as np
import math
import os
from datetime import datetime, timedelta
from vyuha.distance_matrix.cluster_validate import *
from sklearn.cluster import DBSCAN, KMeans
from sklearn import metrics
import time

class Cluster:
    def __init__(self, hub, sales_value_benchmark=45000, distance_benchmark=30000, max_clusters = 30, pincode=[], km_per_hour=20, serving_time=5, time_benchmark=210):
        sales_path = 'vyuha/distance_matrix/output_files/sales/sales.csv'
        distance_matrix_path = 'vyuha/distance_matrix/output_files/{}.csv'.format(hub)
        self.distance_matrix = pd.read_csv(distance_matrix_path)
        print('Distance matrix data found !')
        self.sales = pd.read_csv(sales_path)
        print('Sales data found !')

        previous_month_date = datetime.today() - relativedelta(months=1)
        start_date = current_date = datetime(previous_month_date.year, previous_month_date.month, 1).date()
        today = start_date + relativedelta(months=1) - timedelta(days=1)

        print('Start Date: {}'.format(start_date))
        print('End Date: {}'.format(today))
        
        if len(pincode) != 0:
            print('HERE')
            self.distance_matrix = self.distance_matrix[(self.distance_matrix['from_pincode'].isin(pincode)) & (self.distance_matrix['to_pincode'].isin(pincode))]
        print(self.distance_matrix)
        
        self.km_per_hour = km_per_hour
        self.serving_time = serving_time
        self.time_benchmark = time_benchmark
        
        self.hub = hub
        self.counter = 0
        
        self.sales_value_benchmark = sales_value_benchmark
        self.distance_benchmark = distance_benchmark
        self.max_clusters = max_clusters
        self.vl = Validate(hub = hub, previous_month_date = previous_month_date,start_date = start_date, current_date = current_date, today = today, sales_value_benchmark = sales_value_benchmark, distance_benchmark = distance_benchmark, km_per_hour=km_per_hour, serving_time=serving_time, time_benchmark=time_benchmark)
        # self.vl = Validate(hub, sales_value_benchmark, distance_benchmark, km_per_hour, serving_time, time_benchmark)
        
        
        self.distance_matrix['to_customer_code'] = self.distance_matrix['to_customer_code'].astype('str')
        self.distance_matrix['from_customer_code'] = self.distance_matrix['from_customer_code'].astype('str')
        
        self.distance_matrix.drop(self.distance_matrix[(self.distance_matrix['to_customer_code'] == str(hub))].index, inplace=True)
        self.distance_matrix.drop(self.distance_matrix[(self.distance_matrix['to_customer_code'] == self.distance_matrix['from_customer_code'])].index, inplace=True)
        
        
        self.total_customers = len(set(self.distance_matrix.from_customer_code))
        print('Total Customers:', self.total_customers)
        
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
        
    def make_cluster(self, distance_matrix, current_place=None):
        try:
            print(distance_matrix)
            if current_place == None:
                current_place = self.hub

            print('Cluster: ',self.counter)
            print('Clustering For: ', len(self.customers_clustered))
            # Extra code
#             if self.main_counter == 10:
#                 self.reset()
#                 print('Exit')
#                 return 'Done'
            #Extra code
            print('total customers:',self.total_customers,',', 'total customers:',self.total_customers+self.counter)
            print('Customer Clustered:', len(set(self.customers_clustered)),',', 'Counter:', self.counter,',', 'distance_matrix is empty:', distance_matrix.empty)
            if len(self.customers_clustered) >= self.total_customers+self.counter or self.counter > self.max_clusters or distance_matrix.empty == True:
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
        except Exception as e:
            print('Exception:', str(e))
            self.reset()
            print('Exit')
            return 'Done'
    
    def check(self, customers):
        #Validate
        # print(customers)
        desc = self.vl.validate(customers)
        summary = self.vl.summary()
        print(summary)
        
#         if summary['SALE VALUE'] >= 90:
#         if summary['DAILY SALE VALUE'] <= self.sales_value_benchmark:
#         if summary['COMPLETE TRIP TIME'] <= self.time_benchmark:
        if summary['COMPLETE TRIP TIME'] >= 95:
#         if summary['DAILY TRAVEL DISTANCE'] <= self.distance_benchmark:
            self.summary = summary
            self.desc = desc
            return True
        else:
            return False
        
    
    def reset(self):
        try:
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
        except Exception as e:
            print(str(e))
    
    def next_nearest_customer(self, distance_matrix, current_customer):
        print(current_customer.strip())
        self.latest_distance_matrix = distance_matrix
        result = distance_matrix[distance_matrix['from_customer_code'] == current_customer].sort_values('distance').iloc[0,:]
        return result
    
    def start(self):
        start_time = time.time()
        print('Engine running...')
        self.make_cluster(self.distance_matrix)
        elapsed_time = time.time() - start_time
        print('Elapsed Time:', elapsed_time)
    def export_reports(self):
        final_output = pd.DataFrame(columns=['Cluster', 'Customer'])
        cluster = []
        places = []
        for x in range(len(self.places_dict)):
            for place in self.places_dict[x]:
                places.append(place)
                cluster.append(x)
        print(final_output)
        cluster_report_path = 'vyuha/distance_matrix/output_files/cluster_reports/{}'.format(self.hub)
        if os.path.isdir(cluster_report_path) == False:
            os.makedirs(cluster_report_path)
        cluster_report_path += '/final_cluster_report.csv'

        final_output.to_csv(cluster_report_path, index=True)
        
    def export_summary(self):
        summary_path = 'vyuha/distance_matrix/output_files/summary/{}_summary.txt'.format(self.hub)
        file = open(summary_path, 'w+')
        print(self.summary_dict)
        for i in self.summary_dict:
            try:
                sale_value_pass_rate = self.summary_dict[i]['SALE VALUE']
                distance_value_pass_rate = self.summary_dict[i]['DISTANCE']
                avg_daily_sale_value = self.summary_dict[i]['DAILY SALE VALUE']
                avg_daily_distance = self.summary_dict[i]['DAILY TRAVEL DISTANCE']
                daily_travel_time = self.summary_dict[i]['DAILY TRAVEL TIME']
                complete_trip_time = self.summary_dict[i]['COMPLETE TRIP TIME']

                sentence = 'Cluster: {}\nSale Value Pass: {}%\nDistance Pass: {}%\nAvg Daily Sale: {}\nAvg Daily Distance: {}\nComplete Trip Time Pass: {}%\nDaily Travel Time: {}\n\n'.format(i, sale_value_pass_rate, distance_value_pass_rate, avg_daily_sale_value, avg_daily_distance, complete_trip_time, daily_travel_time)
                file.write(sentence)
            except Exception as e:
                print(str(e))
        file.close()




class dbscan_cluster:
    def __init__(self, unit, df, eps, min_samples):
        self.df = df
        self.unit = unit
        self.eps = eps
        self.min_samples = min_samples

    def start(self):
        df = self.df
        X = df.iloc[:, 4:].values
        dbscan = DBSCAN(eps=self.eps, min_samples=self.min_samples)
        model = dbscan.fit(X)
        labels = model.labels_
        n_cluters = len(set(labels)) - (1 if -1 in labels else 0)
        df['cluster'] = labels
        df['centroid'] = 0

        centroid_lat = df.groupby('cluster').latitude.mean().reset_index()
        centroid_long = df.groupby('cluster').longitude.mean().reset_index()
        centroid_data = centroid_lat.merge(centroid_long, on='cluster')
        centroid_data['Customer Name'] = centroid_data['cluster'].apply(lambda x:'Cluster - ' + str(x))
        centroid_data['Customer Code'] = centroid_data['cluster'].apply(lambda x:'Cluster - ' + str(x))
        centroid_data['distributor_id'] = self.df.iloc[0, 0]
        centroid_data['Distributor Name'] = self.df.iloc[0, 1]
        centroid_data['centroid'] = 1

        df = pd.concat([df, centroid_data])
        df['algo'] = 'dbscan'
        self.result = df
        return df

class kmeans_cluster:
    def __init__(self, unit, df, n_clusters=8, n_init=10, max_iter=300, tol=0.0001, algorithm='lloyd'):
        self.df = df
        self.unit = unit
        self.n_clusters = n_clusters
        self.n_init = n_init
        self.max_iter = max_iter
        self.tol = tol
        self.algorithm = algorithm

    def start(self):
        df = self.df
        X = df.iloc[:, 4:].values
        kmeans = KMeans(n_clusters=self.n_clusters, n_init=self.n_init, max_iter=self.max_iter, tol=self.tol, algorithm=self.algorithm)
        model = kmeans.fit(X)
        labels = model.labels_
        n_cluters = len(set(labels)) - (1 if -1 in labels else 0)
        self.n_features_in_ = model.n_features_in_
        self.cluster_centers_ = model.cluster_centers_
        self.inertia_ = model.inertia_
        df['cluster'] = labels
        df['centroid'] = 0

        centroid_lat = df.groupby('cluster').latitude.mean().reset_index()
        centroid_long = df.groupby('cluster').longitude.mean().reset_index()
        centroid_data = centroid_lat.merge(centroid_long, on='cluster')
        centroid_data['Customer Name'] = centroid_data['cluster'].apply(lambda x:'Cluster - ' + str(x))
        centroid_data['Customer Code'] = centroid_data['cluster'].apply(lambda x:'Cluster - ' + str(x))
        centroid_data['distributor_id'] = self.df.iloc[0, 0]
        centroid_data['Distributor Name'] = self.df.iloc[0, 1]
        centroid_data['centroid'] = 1
        
        df = pd.concat([df, centroid_data])
        df['algo'] = 'kmeans'
        self.df = df
        return self.df