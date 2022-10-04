from operator import concat
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
from vyuha.connection import *

driver,sheeter = sq.apiconnect()
shifts = sq.sheetsToDf(sheeter,spreadsheet_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='Shifts')
shifts['shift_factor'] = shifts['shift_factor'].astype(float)


class Roaster:
    def __init__(self) -> None:
        pass

    def calculate_roaster(self, df):
        dist_list = []
        dept_list = []
        dayname_list = []
        hourly_manpower_list = []
        total_excess_man_hour_list = []
        total_quantum_list = []
        month_list = []
        final_data = pd.DataFrame()
        df['scan_date'] = pd.to_datetime(df['scan_date'])
        df['day_name'] = df['scan_date'].dt.day_name()
        df['month'] = df['scan_date'].dt.strftime('%m-%Y')
        df = df.pivot_table(index=['dist_name', 'month', 'hour', 'day_name', 'department'], values=['manpower', 'total_quantum']).reset_index()
        try:
            for dist in df['dist_name'].unique():
                for month in df['month'].unique():
                    print('month', month)
                    for dept in df['department'].unique():
                        print(dist)
                        for dayname in df['day_name'].unique():
                            print(dayname)
                            status = False
                            hourly_manpower = 1
                            data = df[(df['dist_name'] == dist) & (df['month'] == month) & (df['department'] == dept) & (df['day_name'] == dayname)]
                            while status == False:
                                data['excess_defficiet'] = hourly_manpower - data['manpower']
                                total_man_hour = data['excess_defficiet'].sum()
                                if total_man_hour >= 0:
                                    status = True
                                    total_quantum = data['total_quantum'].sum()
                                else:
                                    hourly_manpower += 1
                            dist_list.append(dist)
                            month_list.append(str(month))
                            dayname_list.append(dayname)
                            hourly_manpower_list.append(hourly_manpower)
                            total_excess_man_hour_list.append(total_man_hour)
                            dept_list.append(dept)
                            total_quantum_list.append(total_quantum)
            final_data['dist'] = dist_list
            final_data['month'] = month_list
            final_data['dept'] = dept_list
            final_data['dayname'] = dayname_list
            final_data['hourly_manpower'] = hourly_manpower_list
            final_data['load'] = total_quantum_list
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
        return final_data
    
    def fetch_tableau_day_wise_roaster(self):
        with server.auth.sign_in(tableau_auth):
            req_option = TSC.RequestOptions()
            workbook = server.workbooks.get_by_id('6bbbabc3-1529-4d5b-9e6b-3eb842c5fa4d')
            server.workbooks.populate_views(workbook)
            req_option.filter.add(
                TSC.Filter(
                    TSC.RequestOptions.Field.Name,
                    TSC.RequestOptions.Operator.Equals,
                    'Roaster Daily Dashboard'
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
            
            with open('vyuha/others/files/output_files/roaster_daywise_tableau_data.csv', 'wb') as f:
                f.write(b''.join(view_item.csv))
                print("-----------------Data Successfully Written to roaster_daywise_tableau_data.csv--------------------")
    
    def get_hourly_roster(self):
        data = self.roaster_daily_data#sq.sheetsToDf(sheeter,spreadsheet_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='Roaster')
        data = data[~data['dept'].isin(['Checking', 'Picking', 'Dispatch'])]
        data.rename(columns={'hourly_manpower':'daily_manpower'}, inplace=True)
        data = data.merge(shifts, left_on='dist', right_on='Distributors', how='left')
        data = data.loc[:, ['dist', 'month', 'dept', 'dayname', 'daily_manpower', 'load', 'duration']]
        
        data['daily_manpower'] = data['daily_manpower'].astype(float)
        data['duration'] = data['duration'].astype(float)
        data['load'] = data['load'].astype(float)

        data['hourly_manpower'] = data['daily_manpower']/data['duration']
        data['hourly_load'] = data['load']/data['duration']

        hourly_roaster = pd.DataFrame()
        for index, row in shifts.iterrows():
            start_hour = int(row['start_hour'])
            end_hour = int(row['end_hour'])
            if end_hour == 23: end_hour = 24
        #     print(row['Distributors'], start_hour, end_hour)
            shift_hours = pd.DataFrame({'shift_hours':[i for i in range(start_hour, end_hour)]})
            dist_data = data[data['dist']==row['Distributors']]
            dist_data = dist_data.merge(shift_hours, how='cross')
            hourly_roaster = pd.concat([hourly_roaster, dist_data])
        hourly_roaster = hourly_roaster.loc[:, ['dist', 'month', 'dept', 'dayname', 'hourly_manpower', 'hourly_load', 'shift_hours']]
        hourly_roaster.rename(columns={'dist':'dist_name', 'month':'scan_date','dept':'department', 'dayname':'day_name', 'hourly_load':'total_quantum', 'hourly_manpower':'manpower', 'shift_hours':'hour'}, inplace=True)
        return hourly_roaster

    def start(self):
        files = [] 
        master_df = pd.DataFrame()
        for file in os.listdir('vyuha/others/files/output_files/'):
            if 'Hour' in file and 'Roaster' not in file:
                df = pd.read_csv('vyuha/others/files/output_files/'+file)
                if 'Picking' in file:
                    df['department'] = 'Picking'
                if 'Checking' in file:
                    df['department'] = 'Checking'
                if 'Dispatch' in file:
                    df['department'] = 'Dispatch'
                master_df = pd.concat([master_df, df])
                files.append(file)
        result = self.calculate_roaster(master_df)
        cols = result.columns
        output = result.merge(shifts, left_on='dist', right_on='Distributors', how='left')
        output['hourly_manpower'] = output['hourly_manpower'] * output['shift_factor']
        output = output.loc[:, cols]
        # output['month'] = output['month'].dt.strftime('%m-%Y')
        # print(output.head())
        # output['month'] = output['month'].astype(str)
        print(output.head())
        print(output.info())
        try:
            self.fetch_tableau_day_wise_roaster()
            df = pd.read_csv('vyuha/others/files/output_files/roaster_daywise_tableau_data.csv')

            df['Measure Values'] = df['Measure Values'].apply(lambda x:str(x).replace(',', ''))
            df['Measure Values'] = df['Measure Values'].astype(float)

            df = df[df['Dist Id'].notna()]
            # df = df[(df['Measure Names'] == 'Ideal Head Count') & (~df['KPI'].isin(['Picklist Qty', 'Picklist Line Items', 'Picklist Dispatched']))]
            df = df[(df['Measure Names'].isin(['Ideal Head Count', 'KPI Actual Value'])) & (~df['KPI'].isin(['Picklist Qty', 'Picklist Line Items', 'Picklist Dispatched']))]
            # df = df.iloc[:, [2, 5, 0, 6, 7]]
            df = pd.pivot_table(df, index=['Distributor Name', 'Month Year', 'Department', 'weekday'], 
               columns=['Measure Names'], values='Measure Values', aggfunc=np.sum).reset_index()
            print(df.head())
            df.rename(columns={'Distributor Name':'dist', 'Month Year':'month', 'Department':'dept', 'weekday':'dayname', 'Ideal Head Count':'hourly_manpower', 'KPI Actual Value':'load'}, inplace=True)
            output = pd.concat([output, df])
            print(df.info())
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
        self.roaster_daily_data = output
        output.to_csv('vyuha/others/files/output_files/Roaster Day Output.csv', index=False)
        output = pd.read_csv('vyuha/others/files/output_files/Roaster Day Output.csv')
        sq.dftoSheetsfast(driver,sheeter,output,sp_nam_id='1vUw629ei6icmRjiZoL6zgcFKRhFcxBBtKUeusqcc7Vg',sh_name='Roaster')
        hourly_roaster = master_df.iloc[:, [1,2,3,9,10,16,17,18,19,20]]
        
        hourly_roaster = hourly_roaster[hourly_roaster['active_hour'] == 1]
        hourly_roaster['scan_date'] = hourly_roaster['scan_date'].dt.to_period('M').dt.to_timestamp()
        hourly_roaster = hourly_roaster.pivot_table(index=['dist_name', 'scan_date', 'department', 'day_name', 'hour'], 
                  values=['total_quantum', 'manpower'], aggfunc=np.mean).reset_index()
        roster_data = self.get_hourly_roster()
        roster_data['scan_date'] = roster_data['scan_date'].apply(lambda x:datetime.strptime(x, '%m-%Y'))
        print('Roaster Data')
        print(roster_data.head())
        hourly_data = pd.concat([hourly_roaster, roster_data])
        hourly_data['scan_date'] = hourly_data['scan_date'].dt.date
        hourly_data.to_csv('vyuha/others/files/output_files/Roster Hourly Data.csv', index=False)
        # exceltodl3(hourly_data, 'adhoc.ops_roster_hourly_manpower_load')
        # sq.dftoSheetsfast(driver,sheeter,hourly_data,sp_nam_id='1PUC2oqtmgzFiBT7oL9vNP_vltwHA3so7WXHaCQ5l1TA',sh_name='Roaster Hourly')
        return output