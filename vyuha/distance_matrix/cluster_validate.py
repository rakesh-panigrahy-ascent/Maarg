import pandas as pd
import numpy as np
import math
import os
from datetime import datetime, timedelta

class Validate:
    def __init__(self, hub, sales_value_benchmark = 40000, distance_benchmark = 30000):
        distance_matrix_path = 'vyuha/distance_matrix/output_files/{}_distance_matrix.csv'.format(hub)
        sales_path = 'vyuha/distance_matrix/output_files/sales/{}.csv'.format(hub)
        distance_matrix = pd.read_csv(distance_matrix_path)
        sales = pd.read_csv(sales_path)
        
        distance_matrix['to_customer_code'] = distance_matrix['to_customer_code'].astype('str')
        distance_matrix['from_customer_code'] = distance_matrix['from_customer_code'].astype('str')
        
        self.distance_matrix = distance_matrix
        self.sales = sales
        self.sales['tagdt'] = pd.to_datetime(self.sales['tagdt'])
        self.sales['acno'] = self.sales['acno'].astype('str')
        self.test_cases = {}
        self.sales_value_benchmark = sales_value_benchmark
        self.distance_benchmark = distance_benchmark

    def validate(self, customers = []):
        hub = customers[0]
        self.customers = customers
        today = datetime.today().date()
        start_date = current_date = today - timedelta(days=30)
        
        while current_date <= today:
            sale_value, sale_test_case, customers_served, customers_served_count = self.validate_sales(current_date)
            customers_served = list(customers_served)
            customers_served.insert(0, hub)
            distance, distance_test_case = self.validate_distance(current_date, customers_served, customers_served_count)
            self.test_cases[str(current_date)] = {'sale':sale_value, 'sale_test_case':sale_test_case, 'distance':distance, 'distance_test_case':distance_test_case, 'customers':customers_served, 'customers_count':customers_served_count}
            current_date += timedelta(days=1)
        self.final_data = pd.DataFrame(self.test_cases).transpose()
        return self.final_data
            
    def validate_sales(self, current_date):
        sales_data = self.sales
        customers = self.customers
        case = 'Fail'
        total_sales = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date)].amount.sum()
        customers_served_count = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date)].acno.nunique()
        customers_served = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date)].acno.unique()
        
        if total_sales <= self.sales_value_benchmark:
            case = 'Pass'
        return total_sales, case, customers_served, customers_served_count
    
    def validate_distance(self, current_date, customers_served, customers_served_count):
#         print(customers_served)
        customers_served = list(map(str, customers_served))
#         print(customers_served)
        sales_data = self.sales
        customers = customers_served #self.customers
        distance_matrix = self.distance_matrix
        case = 'Fail'
        total_distance = 0
        for i in range(len(customers)-1):
            try:
                distance = distance_matrix[(distance_matrix['from_customer_code'] == str(customers[i])) & (distance_matrix['to_customer_code'] == str(customers[i+1]))]['distance'].values[0]
            except:
                distance = 0
            total_distance += distance
            
        if total_distance <= self.distance_benchmark:
            case = 'Pass'
        return total_distance, case
    
    def summary(self):
        final_data = self.final_data
        sale_test_case = round(final_data[final_data['sale_test_case']=='Pass'].count().values[0]/final_data.count().values[0],2)*100
        distance_test_case = round(final_data[final_data['distance_test_case']=='Pass'].count().values[0]/final_data.count().values[0],2)*100
        daily_sale_value = round(final_data['sale'].median(),2)
        daily_travel_distance = round(final_data['distance'].median(),2)
        return {'SALE VALUE':sale_test_case, 'DISTANCE':distance_test_case, 'DAILY SALE VALUE':daily_sale_value, 'DAILY TRAVEL DISTANCE':daily_travel_distance}