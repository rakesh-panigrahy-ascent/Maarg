from tracemalloc import start
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import datetime as dtime
warnings.filterwarnings('ignore')
import math
import sys
import os
from Maarg.settings import *
import vyuha.sheetioQuicks as sq
from vyuha.others.roaster_planning import *

driver,sheeter = sq.apiconnect()

class CapacityPlanning:
    def __init__(self, data, start_date, end_date, target_kpi, start_hour = 10, end_hour = 19, kpi_name='total_line_items', split_hour = 2, kpi='Picking', serviceability=0.9, total_projection_value=0, v1_value = 0, v2_value = 0, roaster='false'):
        self.operating_hours = pd.read_excel('vyuha/others/files/input_files/operating_hours.xlsx', 'Sheet1')
        data['time_slot'] = pd.to_datetime(data['time_slot'])
        data['scan_date'] = pd.to_datetime(data['scan_date'])

        if kpi == 'Checking':
            data['time_slot'] = data['time_slot'] + timedelta(hours=1)
            data['scan_data'] = data['time_slot'].dt.date
        
        if kpi == 'Dispatch':
            data['time_slot'] = data['time_slot'] + timedelta(hours=2)
            data['scan_data'] = data['time_slot'].dt.date

        self.data = data
        self.start_date = start_date
        self.end_date = end_date
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.target_kpi = target_kpi
        self.kpi_name = kpi_name
        self.split_hour = split_hour
        self.kpi = kpi
        self.serviceability = serviceability*100
        self.daily_master_summary =  None
        self.dist_data = {}
        self.total_projection_value = total_projection_value
        self.v1_value = v1_value
        self.v2_value = v2_value
        self.roaster = roaster
        
        self.report_prefix = None 
        if total_projection_value != 0:
            self.report_prefix = 'Projected'


    def rectify_data(self, data):
        try:
            dtime_list = []
            current_date = datetime.combine(self.start_date, datetime.min.time())
            while current_date <= datetime.combine(self.end_date, dtime.time(23)):
                current_date += timedelta(hours=1)
                dtime_list.append(current_date)
            capacity_meta_data = pd.DataFrame({'time_slot':dtime_list})
            
            consol_data = capacity_meta_data.merge(data, on='time_slot', how='left')

            consol_data['n_distributor_id'].fillna(consol_data['n_distributor_id'].median(), inplace=True) 
            consol_data['dist_name'].fillna(consol_data['dist_name'].value_counts().index[0], inplace=True)
            consol_data['scan_date'] = pd.to_datetime(consol_data['time_slot'].dt.date)
            consol_data['challan_count'].fillna(0, inplace=True)
            consol_data['total_line_items'].fillna(0, inplace=True)
            consol_data['item_value'].fillna(0, inplace=True)
            consol_data['quantity'].fillna(0, inplace=True)
            
            
            consol_data['n_distributor_id'].astype(int)
            consol_data['challan_count'].astype(int)
            consol_data['total_line_items'].astype(int)
            consol_data['quantity'].astype(int)
            
            return consol_data
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
        
    def process_data(self, dist_name):
        try:
            self.start_hour = self.operating_hours[self.operating_hours['Distributors'] == dist_name].loc[:, 'start_hour'].values[0]
            self.end_hour = self.operating_hours[self.operating_hours['Distributors'] == dist_name].loc[:, 'end_hour'].values[0]
            print(dist_name)
            start_date = self.start_date
            end_date = self.end_date
            start_hour = self.start_hour
            end_hour = self.end_hour
            target_kpi = self.target_kpi
            kpi_name = self.kpi_name

            data = self.data[(self.data['scan_date'].dt.date >= start_date) & (self.data['scan_date'].dt.date <= end_date) & (self.data['dist_name'] == dist_name)]
            
            data = self.rectify_data(data)
            
            data['hour'] = data['time_slot'].dt.hour
            data['active_hour'] = data['hour'].apply(lambda x:1 if x>= start_hour and x<=end_hour else 0)
            data.sort_values(by=['dist_name', 'time_slot'], inplace=True)
            
            day_hours = np.arange(0, 24)
            date_range = sorted(data.scan_date.dt.date.unique())
            
            data['previous_day_pending_quantum'] = 0
            pre_inactive_hours = 0
            post_inactive_hours = 0
            
            for dt in date_range:
                pre_inactive_hours = data[(data['scan_date'].dt.date == dt) & (data['active_hour'] == 0) & (data['hour'] < start_hour)][kpi_name].sum()
                pending_tasks = pre_inactive_hours + post_inactive_hours
                post_inactive_hours = data[(data['scan_date'].dt.date == dt) & (data['active_hour'] == 0) & (data['hour'] > end_hour)][kpi_name].sum()
                data.loc[(data['hour']==start_hour) & (data['scan_date'].dt.date==dt), ['previous_day_pending_quantum']] = pending_tasks
            data['actual_quantum'] = data[kpi_name]/self.split_hour
            data['left_quantum'] = data[kpi_name] - (data[kpi_name]/self.split_hour)

            temp = data.loc[:, ['n_distributor_id', 'time_slot', 'actual_quantum']]
            temp['time_slot'] = temp['time_slot'] + timedelta(hours=1)
            temp.rename(columns={'actual_quantum':'current_quantum'}, inplace=True)
            result = data.merge(temp, on=['n_distributor_id', 'time_slot'], how='left')
            result.fillna(0, inplace=True)
            
            data = result
            
            temp = data.loc[:, ['n_distributor_id', 'time_slot', 'left_quantum']]
            temp['time_slot'] = temp['time_slot'] + timedelta(hours=2)
            temp.rename(columns={'left_quantum':'pending_quantum'}, inplace=True)
            result = data.merge(temp, on=['n_distributor_id', 'time_slot'], how='left')
            result.fillna(0, inplace=True)

            result['total_quantum'] = result['previous_day_pending_quantum']+result['current_quantum']+result['pending_quantum']
            result.loc[(result['active_hour'] == 0), ['total_quantum']] = 0
            result.loc[(result['hour'] == start_hour), ['total_quantum']] = result['previous_day_pending_quantum'] #result['total_quantum'] - result['current_quantum']
            result['manpower'] = np.round(result['total_quantum']/target_kpi, 0)
            # self.check_man_power(result, dist_name)
            self.dist_data[dist_name] = result
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))


    
    def check_man_power(self, data, dist_name):
        date_range = sorted(data.scan_date.dt.date.unique())
        success_record_count = np.int0(self.end_hour - self.start_hour - np.round((self.end_hour - self.start_hour)*0.1))
        fail_record_count = np.int0((self.end_hour - self.start_hour)*0.1)
        self.dt_list = []
        self.manpower_list = []
        self.dist_list = []
        for dt in date_range:
            temp = data[data['scan_date'].dt.date == dt]
            manpower = temp['manpower'].nlargest(np.int0((self.end_hour - self.start_hour)*0.1)+1).iloc[np.int0((self.end_hour - self.start_hour)*0.1)]
            self.dist_list.append(dist_name)
            self.dt_list.append(dt)
            self.manpower_list.append(manpower)
        self.daily_summary = pd.DataFrame({
            'Distributor':self.dist_list,
            'Date':self.dt_list,
            'ManPower':self.manpower_list
        })
        self.daily_summary['Date'] = pd.to_datetime(self.daily_summary['Date'])
        
        if self.daily_master_summary is None:
            self.daily_master_summary = self.daily_summary
        else:
            self.daily_master_summary = pd.concat([self.daily_master_summary, self.daily_summary])
        self.daily_master_summary['WeekDay'] = self.daily_master_summary['Date'].dt.day_name()

    def export_daily_summary(self):
        output_dir = 'vyuha/others/files/output_files/'
        output_file = output_dir + 'Capacity Planning Daily Summary.csv'
        if os.path.exists('vyuha/others/files/output_files/') == False:
            os.makedirs('vyuha/others/files/output_files/')
        self.daily_master_summary.to_csv(output_file, index=False)

    def export_weekday_summary(self):
        output_dir = 'vyuha/others/files/output_files/'
        output_file = output_dir + 'Capacity Planning Weekday Summary.csv'
        daily_summary = self.daily_master_summary
        desc = daily_summary.groupby(['Distributor', 'WeekDay']).describe().reset_index()
        if os.path.exists('vyuha/others/files/output_files/') == False:
            os.makedirs('vyuha/others/files/output_files/')
        desc.to_csv(output_file, index=False)

    def export_hourly_data(self):
        output_dir = 'vyuha/others/files/output_files/'
        output_file = output_dir + '{} - Capacity Planning {} Hourly Data.csv'.format(self.start_date.strftime('%B-%y'), self.kpi)
        main_data = pd.DataFrame()
        for unit in self.dist_data.keys():
            main_data = pd.concat([main_data, self.dist_data[unit]])
        if os.path.exists('vyuha/others/files/output_files/') == False:
            os.makedirs('vyuha/others/files/output_files/')
        main_data.to_csv(output_file, index=False)

    def export_roaster_data(self):
        input_dir = 'vyuha/others/files/output_files/'
   
        input_file = input_dir+'{} - Capacity Planning {} Hourly Data.csv'.format(self.start_date.strftime('%B-%y'), self.kpi)
        hour_data = pd.read_csv(input_file)
        hour_data['scan_date'] = pd.to_datetime(hour_data['scan_date'])
        hour_data['weekday'] = hour_data['scan_date'].dt.day_name()
        roaster_result = pd.DataFrame()
        for day in hour_data['weekday'].unique():
            print(day)
            data = hour_data[hour_data['weekday'] == day]
            roaster_result = pd.concat([roaster_result, self.export_serviceability(data, day)])
        
        for hour in hour_data['hour'].unique():
            data = hour_data[hour_data['hour'] == hour]
            result = self.export_serviceability(data, hour)
            result['ManPower'] = result['HeadCount']
            roaster_result = pd.concat([roaster_result, result])
            

        return roaster_result
        

    # def merge_refine_roaster_data(self):


    def export_serviceability(self, hour_data=None, day=''):
        if day == '':
            input_dir = 'vyuha/others/files/output_files/'
    
            input_file = input_dir+'{} - Capacity Planning {} Hourly Data.csv'.format(self.start_date.strftime('%B-%y'), self.kpi)
            hour_data = pd.read_csv(input_file)
        
        hour_data  = hour_data[hour_data['active_hour'] == 1]
        hour_data['time_slot'] = pd.to_datetime(hour_data['time_slot'])
        distributors = hour_data['dist_name'].unique()
        
        dist_list = []
        headcount_list = []
        success_rate_list = []
        for dist in distributors:
            df = hour_data[hour_data['dist_name'] == dist]
            df.sort_values(by=['time_slot'], inplace=True)
            df['manpower'] = df['manpower'].astype(int)
            for i in sorted(df['manpower'].unique()):
                # print(dist, i, (df[df['manpower'] <= i].count()[0]/df.count()[0])*100)
                dist_list.append(dist)
                headcount_list.append(i)
                success_rate_list.append((df[df['manpower'] <= i].count()[0]/df.count()[0])*100)
        summary = pd.DataFrame({'Dist':dist_list, 'HeadCount':headcount_list, 'Serviceability':success_rate_list})
        output_dir = 'vyuha/others/files/output_files/'
        if day == '':
            output_file = output_dir + '{} - Capacity Planning {} Serviceability.csv'.format(self.start_date.strftime('%B-%y'), self.kpi)
        else:
            output_file = output_dir + '{} - {} - Roaster {} Serviceability.csv'.format(day, self.start_date.strftime('%B-%y'), self.kpi)
        
        if os.path.exists(output_dir) == False:
            os.makedirs(output_dir)

        summary.to_csv(output_file, index=False)
        return self.export_serviceability_summary(summary, self.serviceability, day)


    def export_serviceability_summary(self, data, serviceabilty, day=''):
        print('serviceabilty', serviceabilty)
        data = data[data['Serviceability'] >= float(serviceabilty)].sort_values(by=['Dist','Serviceability'])
        data.drop_duplicates(subset=['Dist'], inplace=True)
        data['dt'] = self.start_date.strftime('%B-%y')
        data['kpi'] = self.kpi
        operating_hours = self.operating_hours.copy()
        operating_hours.rename(columns={'Distributors':'Dist'}, inplace=True)
        data = data.merge(operating_hours, on='Dist', how='left', suffixes=['', ''])
        # data['duration'] = data['end_hour'] - data['start_hour']
        data['ManPower'] = data['shift_factor']*data['HeadCount']
        output_dir = 'vyuha/others/files/output_files/'
        
        if os.path.exists('vyuha/others/files/output_files/') == False:
            os.makedirs('vyuha/others/files/output_files/')

        if day == '':
            output_file = output_dir + '{} - {} - Service Level Result.csv'.format(self.start_date.strftime('%B-%y'), self.kpi)
            data.to_csv(output_file, index=False)
        else:
            data['Day/Hour'] = day
            return data

    def fetch_tableau_data(self):
        with server.auth.sign_in(tableau_auth):
            req_option = TSC.RequestOptions()
            workbook = server.workbooks.get_by_id('6bbbabc3-1529-4d5b-9e6b-3eb842c5fa4d')
            server.workbooks.populate_views(workbook)
            req_option.filter.add(
                TSC.Filter(
                    TSC.RequestOptions.Field.Name,
                    TSC.RequestOptions.Operator.Equals,
                    'Capacity Planning Dashboard Ideal'
                )
            )
            
            all_views, pagination_item = server.views.get(req_option)    
            if not all_views:
                raise LookupError("View with the specified name was not found.")
            # print(all_views)
            view_item = all_views[0]
            # print(view_item)
            csv_req_option = TSC.CSVRequestOptions()
            server.views.populate_csv(view_item, csv_req_option)
            
            with open('vyuha/others/files/output_files/Capacity_Planning_V1.csv', 'wb') as f:
                f.write(b''.join(view_item.csv))
                print("-----------------Data Successfully Written to Capacity_Planning.csv--------------------")

    def export_final_data(self, start_date, end_date):
        v1_capacity_planning = pd.read_csv('vyuha/others/files/output_files/Capacity_Planning_V1.csv')
        v1_capacity_planning = v1_capacity_planning[v1_capacity_planning['Distributor Name'].notnull()]
        v1_capacity_planning.rename(columns={'Distributor Name':'Dist'}, inplace=True)
        v1_capacity_planning = v1_capacity_planning[v1_capacity_planning['Date'] != 'All']
        v1_capacity_planning['Date'] = pd.to_datetime(v1_capacity_planning['Date'])


        v2_capacity_planning = None
        for capacity_csv in os.listdir('vyuha/others/files/output_files/'):
            if 'Result' not in capacity_csv or 'Roaster' in capacity_csv:
                continue

            capacity_v2_df = pd.read_csv('vyuha/others/files/output_files/{}'.format(capacity_csv))
            capacity_v2_df.rename(columns={'dt':'Date'}, inplace=True)
        #     print(capacity_v2_df.columns)
            if 'Dispatch' in capacity_csv:
                kpi_name = 'Dispatch-Sorting'
            elif 'Checking' in capacity_csv:
                kpi_name = 'Checking-Checking & Packing'
            elif 'Picking' in capacity_csv:
                kpi_name = 'Store-Picking'
            capacity_v2_df['Department'] = kpi_name
            capacity_v2_df['Measure Names'] = 'Ideal Head Count'
            capacity_v2_df['Date'] = pd.to_datetime(capacity_v2_df['Date'], format='%B-%y')
            capacity_v2_df = capacity_v2_df[['Dist', 'Department', 'Measure Names', 'Date', 'ManPower']]
            if v2_capacity_planning is None:
                v2_capacity_planning = capacity_v2_df
            else:
                v2_capacity_planning = pd.concat([v2_capacity_planning, capacity_v2_df])
        final_df = v1_capacity_planning.merge(v2_capacity_planning, on=['Dist', 'Department', 'Measure Names', 'Date'], how='left')
        final_df['ManPower'].fillna(0, inplace=True)
        final_df['Measure Values'] = final_df['Measure Values'].apply(lambda x:float(''.join(x.split(','))))
        final_df['Measure Values'] = final_df['Measure Values'] + final_df['ManPower']
        final_df.drop(columns=['ManPower'], inplace=True)
        final_df['Date'] = final_df['Date'].dt.date
        final_df = final_df[(final_df['Date'] >= start_date) & (final_df['Date'] <= end_date)]
        final_df = final_df.pivot_table(index=['Date', 'Department', 'Dist', 'KPI', 'MIS Dept'], columns=['Measure Names'], values='Measure Values').reset_index()
        final_df['Ideal Cost'] = final_df['Ideal Head Count'] * final_df['Ideal Avg Salary']
        if os.path.exists('vyuha/others/files/output_files/') == False:
            os.makedirs('vyuha/others/files/output_files/')
        final_df.to_csv('vyuha/others/files/output_files/Output.csv', index=False)
        if self.report_prefix == None:
            sq.dftoSheetsfast(driver,sheeter,final_df,sp_nam_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='Sheet1')
        else:
            revenue_Data = sq.sheetsToDf(sheeter,spreadsheet_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='revenue data')
            revenue_Data['Date'] = pd.to_datetime(revenue_Data['Date'])
            final_df['Date'] = pd.to_datetime(final_df['Date'])
            revenue_Data.rename(columns={'dist_name':'Dist'}, inplace=True)
            
            col = final_df.columns
            final_data = final_df.merge(revenue_Data, on=['Dist', 'Date'], how='left')
            final_data['Total'] = final_data['Total'].astype(float)
            final_data['V1'] = final_data['V1'].astype(float)
            final_data.loc[~final_data['KPI'].isin(['Picklist Line Items', 'Picklist Qty', 'Picklist Dispatched']), 'Monthly Actual Value'] = (final_data['Monthly Actual Value'] / final_data['V1'])*self.v1_value
            final_data.loc[~final_data['KPI'].isin(['Picklist Line Items', 'Picklist Qty', 'Picklist Dispatched']), 'Daily Actual Value'] = final_data['Monthly Actual Value']/26
            final_data.loc[~final_data['KPI'].isin(['Picklist Line Items', 'Picklist Qty', 'Picklist Dispatched']), 'Ideal Head Count'] = (final_data['Ideal Head Count'] / final_data['V1'])*self.v1_value
            final_data.loc[~final_data['KPI'].isin(['Picklist Line Items', 'Picklist Qty', 'Picklist Dispatched']), 'Ideal Cost'] = final_data['Ideal Head Count'] * final_data['Ideal Avg Salary']
            final_data = final_data[col]
            final_data['Date'] = final_data['Date'].dt.date
            # print('final_data')
            # print(final_data)
            # final_data.to_csv('Projected Output.csv', index=False)
            sq.dftoSheetsfast(driver,sheeter,final_data,sp_nam_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='Projected')
        
    def start(self):
        for dist in self.data['dist_name'].unique():
            try:
                self.process_data(dist)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno, str(e))
        self.export_hourly_data()
        if self.roaster == 'false':
            self.export_serviceability()