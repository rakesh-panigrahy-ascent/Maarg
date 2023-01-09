import pandas as pd
import numpy as np
import math
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class Validate:
    def __init__(self, hub, previous_month_date,start_date, current_date, today, sales_value_benchmark = 40000, distance_benchmark = 30000, km_per_hour=20, serving_time=5, time_benchmark=210):
        distance_matrix_path = 'vyuha/distance_matrix/output_files/{}.csv'.format(hub)
        sales_path = 'vyuha/distance_matrix/output_files/sales/sales.csv'
        distance_matrix = pd.read_csv(distance_matrix_path)
        sales = pd.read_csv(sales_path)
        
        distance_matrix['to_customer_code'] = distance_matrix['to_customer_code'].astype('str')
        distance_matrix['from_customer_code'] = distance_matrix['from_customer_code'].astype('str')
        
        self.serving_time = serving_time
        self.km_per_hour = km_per_hour
        self.time_benchmark = time_benchmark
        
        self.distance_matrix = distance_matrix
        self.sales = sales
        self.sales['tagdt'] = pd.to_datetime(self.sales['tagdt'])
        self.sales['acno'] = self.sales['acno'].astype('str')
        self.test_cases = {}
        self.sales_value_benchmark = sales_value_benchmark
        self.distance_benchmark = distance_benchmark

        self.previous_month_date = previous_month_date
        self.start_date = start_date
        self.current_date = current_date
        self.today = today

    def validate(self, customers = []):
        hub = customers[0]
        print('Hub:', hub)
        self.customers = customers
        previous_month_date = self.previous_month_date
        start_date = self.start_date
        current_date = self.current_date
        today = self.today
        counter = 0
        while current_date <= today:
            shift1_time_case = 'Fail'
            shift2_time_case = 'Fail'
            
            total_sales, case, customers_served, customers_served_count, shift1_sales, shift1_case, shift1_customers_served, shift1_customers_served_count, shift2_sales, shift2_case, shift2_customers_served, shift2_customers_served_count = self.validate_sales(current_date)
            customers_served = list(customers_served)
            shift1_customers_served = list(shift1_customers_served)
            shift2_customers_served = list(shift2_customers_served)
            
            shift1_customers_serving_time = shift1_customers_served_count * self.serving_time
            shift2_customers_serving_time = shift2_customers_served_count * self.serving_time
            
            customers_served.insert(0, hub)
            shift1_customers_served.insert(0, hub)
            shift2_customers_served.insert(0, hub)
            
            #Time and Distance Shift1
            shift1_up_distance, shift1_down_distance, shift1_total_distance, shift1_distance_case = self.validate_distance(current_date, shift1_customers_served, shift1_customers_served_count)
            
            shift1_complete_time = (shift1_total_distance/(self.km_per_hour*1000))*60
            shift1_down_time = (shift1_down_distance/(self.km_per_hour*1000))*60
            shift1_up_time = (shift1_up_distance/(self.km_per_hour*1000))*60
            shift1_complete_trip_time = shift1_complete_time + shift1_customers_serving_time
            
            if shift1_complete_trip_time <= self.time_benchmark:
                shift1_time_case = 'Pass'
            
            self.test_cases[str(current_date)+'_shift1'] = {'sale':shift1_sales, 'sale_test_case':shift1_case, 'up_distance':shift1_up_distance, 'down_distance':shift1_down_distance, 'total_distance':shift1_total_distance, 'distance_test_case':shift1_distance_case, 'customers':shift1_customers_served, 'customers_count':shift1_customers_served_count, 'uptime':shift1_up_time, 'downtime':shift1_down_time, 'complete_trip_time':shift1_complete_trip_time, 'time_test_case':shift1_time_case}
            #Time and Distance Shift1
            
            #Time and Distance Shift2
            shift2_up_distance, shift2_down_distance, shift2_total_distance, shift2_distance_case = self.validate_distance(current_date, shift2_customers_served, shift2_customers_served_count)
            
            shift2_complete_time = (shift2_total_distance/(self.km_per_hour*1000))*60
            shift2_down_time = (shift2_down_distance/(self.km_per_hour*1000))*60
            shift2_up_time = (shift2_up_distance/(self.km_per_hour*1000))*60
            shift2_complete_trip_time = shift2_complete_time + shift2_customers_serving_time
            
            if shift2_complete_trip_time <= self.time_benchmark:
                shift2_time_case = 'Pass'
            
            self.test_cases[str(current_date)+'_shift2'] = {'sale':shift2_sales, 'sale_test_case':shift2_case, 'up_distance':shift2_up_distance, 'down_distance':shift2_down_distance, 'total_distance':shift2_total_distance, 'distance_test_case':shift2_distance_case, 'customers':shift2_customers_served, 'customers_count':shift2_customers_served_count, 'uptime':shift2_up_time, 'downtime':shift2_down_time, 'complete_trip_time':shift2_complete_trip_time, 'time_test_case':shift2_time_case}
            #Time and Distance Shift2
            
            current_date += timedelta(days=1)
        self.final_data = pd.DataFrame(self.test_cases).transpose()
        return self.final_data
            
    def validate_sales(self, current_date):
#         print(current_date)
        sales_data = self.sales
        customers = self.customers
        case = 'Fail'
        shift1_case = 'Fail'
        shift2_case = 'Fail'
        
        total_sales = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date)].amount.sum()
        
        shift1_sales = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date) & (sales_data['shift'] == 'Shift 1')].amount.sum()
        shift2_sales = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date) & (sales_data['shift'] == 'Shift 2')].amount.sum()
        
        shift1_customers_served_count = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date) & (sales_data['shift'] == 'Shift 1')].acno.nunique()
        shift1_customers_served = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date) & (sales_data['shift'] == 'Shift 1')].acno.unique()
        
        shift2_customers_served_count = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date) & (sales_data['shift'] == 'Shift 2')].acno.nunique()
        shift2_customers_served = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date) & (sales_data['shift'] == 'Shift 2')].acno.unique()
        
        customers_served_count = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date)].acno.nunique()
        customers_served = sales_data[(sales_data['acno'].isin(customers)) & (sales_data['tagdt'].dt.date == current_date)].acno.unique()
    
        if total_sales <= self.sales_value_benchmark:
            case = 'Pass'
                 
        if shift1_sales <= self.sales_value_benchmark:
            shift1_case = 'Pass'
            
        if shift2_sales <= self.sales_value_benchmark:
            shift2_case = 'Pass'
        
        return total_sales, case, customers_served, customers_served_count, shift1_sales, shift1_case, shift1_customers_served, shift1_customers_served_count, shift2_sales, shift2_case, shift2_customers_served, shift2_customers_served_count
    
    def validate_distance(self, current_date, customers_served, customers_served_count):
        customers_served = list(map(str, customers_served))
        sales_data = self.sales
        customers = customers_served
        distance_matrix = self.distance_matrix
        case = 'Fail'
        total_distance = 0
        up_distance = 0
        down_distance = 0
        for i in range(len(customers)-1):
            try:
                distance = distance_matrix[(distance_matrix['from_customer_code'] == str(customers[i])) & (distance_matrix['to_customer_code'] == str(customers[i+1]))]['distance'].values[0]
#                 print('From:', customers[i])
#                 print('To:', customers[i+1])
#                 print(distance)
            except Exception as e:
                print('Exception:', e)
                print('From:', customers[i])
                print('To:', customers[i+1])
                distance = 0
            up_distance += distance
            
        try:
            down_distance = distance_matrix[(distance_matrix['from_customer_code'] == str(customers[-1])) & (distance_matrix['to_customer_code'] == str(customers[0]))]['distance'].values[0]
        except:
            down_distance = total_distance
            
        total_distance = up_distance + down_distance
            
        if total_distance <= self.distance_benchmark:
            case = 'Pass'
        return up_distance, down_distance, total_distance, case
    
    def summary(self):
        final_data = self.final_data
        sale_test_case = round(final_data[final_data['sale_test_case']=='Pass'].count().values[0]/final_data.count().values[0],2)*100
        distance_test_case = round(final_data[final_data['distance_test_case']=='Pass'].count().values[0]/final_data.count().values[0],2)*100
        time_test_case = round(final_data[final_data['time_test_case']=='Pass'].count().values[0]/final_data.count().values[0],2)*100
        
        daily_sale_value = round(final_data['sale'].median(),2)
        daily_travel_distance = round(final_data['total_distance'].median(),2)
        daily_travel_time = round(final_data['complete_trip_time'].median(),2)
        return {'SALE VALUE':sale_test_case, 'DISTANCE':distance_test_case, 'DAILY SALE VALUE':daily_sale_value, 'DAILY TRAVEL DISTANCE':daily_travel_distance, 'DAILY TRAVEL TIME':daily_travel_time, 'COMPLETE TRIP TIME':time_test_case}