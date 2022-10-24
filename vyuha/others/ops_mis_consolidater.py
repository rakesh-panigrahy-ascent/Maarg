import sys
import os
sys.path.insert(0, os.getcwd())
from datetime import timedelta, date, datetime
import pandas as pd
import vyuha.sheetioQuicks as sq
from sqlalchemy import create_engine, text
import numpy as np
from dateutil.relativedelta import relativedelta
import calendar
from calendar import monthrange
import shutil
import logging
logging.basicConfig(filename='vyuha/distance_matrix/log/distance_matrix.log', level=logging.INFO)

driver,sheeter = sq.apiconnect()

unit_map = sq.sheetsToDf(sheeter,spreadsheet_id='15D-4rRRzzMxcuUsOXk_Rp8Gs4_smLXk5PrpTPlwvv6Y',sh_name='Unit Map')
conf_index_file = os.path.join(os.path.dirname(__file__))+'/Conf/MIS Index.xlsx'
d = 1


class DataFill:
    def __init__(self, sheet_name):
        self.fill_data_status = True
        self.message = ''
        self.sheet_name = sheet_name
        try:
            self.salary_sheet = pd.ExcelFile(os.path.join(os.path.dirname(__file__), 'mis/input_files/Ops Salary Sheet.xlsx'))
            try:
                self.salary_sheet = pd.read_excel(self.salary_sheet, self.sheet_name)
                print('Sheet Found: {}'.format(sheet_name))
            except:
                self.message = 'Sheet Not Found'
                self.fill_data_status = False
        except Exception as e:
            self.message = 'File Not Found !'
            self.fill_data_status = False

    def get_data(self, section):
        try:
            salary_sheet = self.salary_sheet
            if self.fill_data_status == True:
                if section == 'Present Head Count':
                    result = salary_sheet.groupby(['Unit', 'Department -NEW','Sub-Department-NEW'])['Present Headcount'].sum().reset_index()
                    result.rename(columns={'Present Headcount':section}, inplace=True)
                else:
                    result = salary_sheet.groupby(['Unit', 'Department -NEW','Sub-Department-NEW']).Salary.sum().reset_index()
                    result.rename(columns={'Salary':section}, inplace=True)
            else:
                result = self.message
        except Exception as e:
            result = 'NA'
        return result


class OpsMIS:
    def __init__(self):
        self.status = True

    def extract_ops_salary_sheet(self):
        sections = ['Total Salary', 'Present Head Count']
        for section in sections:
            try:
                master_salary_df = pd.DataFrame()
                
                print()
                print()
                print()
                print('Extracting Ops Salary...')

                today = datetime.today()
                current_month = datetime.today().month
                current_year = datetime.today().year

                if current_month in (1,2,3,4):
                    start_year = current_year - 1
                else:
                    start_year = current_year

                current_date = start_date = datetime(start_year, 4, 1)
                
                while current_date <= today:
                    dfill = DataFill(current_date.strftime("%b-%Y"))
                    if dfill.fill_data_status == False:
                        current_date = current_date + relativedelta(months=+1)
                        continue

                    print('MONTH: {}'.format(current_date.strftime("%b-%Y")))

                    result = dfill.get_data(section)

                    salary_df = pd.DataFrame(result)
                    salary_df['Month'] = current_date
                    master_salary_df = pd.concat([master_salary_df,salary_df], ignore_index= True)

                    current_date = current_date + relativedelta(months=+1)

                master_salary_df.rename(columns={'Unit':'unit_name','Month':'month'},inplace= True)
                unit_map_copy = unit_map[['OPS Salary', 'OPS MIS']]
                unit_map_copy.drop_duplicates(subset=['OPS Salary'], inplace=True)

                final_df = master_salary_df.merge(unit_map_copy, left_on='unit_name', right_on='OPS Salary', how='left')
                final_df['unit_name'] = final_df['OPS MIS']
                final_df.drop(columns=['OPS Salary', 'OPS MIS'], inplace=True)
                final_df = final_df.groupby(by=['unit_name', 'Department -NEW', 'Sub-Department-NEW', 'month'])[section].sum().reset_index()

                final_df_department_level = final_df.groupby(by=['unit_name', 'Department -NEW', 'month'])[section].sum().reset_index()
                
                if section == 'Present Head Count':
                    final_df['Working Days'] = final_df['month'].apply(self.get_working_days)
                    final_df['Worked Mandays'] = final_df[section] * final_df['Working Days']
                    

                    # Department level
                    final_df_department_level['Working Days'] = final_df_department_level['month'].apply(self.get_working_days)
                    final_df_department_level['Worked Mandays'] = final_df_department_level[section] * final_df_department_level['Working Days']
                    # Department level

                    kpi_list = [section, 'Worked Mandays', 'Working Days']
                    for kpi in kpi_list:
                        self.format_ops_salary_export_data(kpi, final_df, 0)
                        self.format_ops_salary_export_data(kpi, final_df_department_level, 1)
                else:
                    final_df = self.format_ops_salary_export_data(section, final_df)
                    self.format_ops_salary_export_data(section, final_df_department_level, 1)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno, str(e))
                logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
                self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
                self.status = False

    def get_working_days(self, x):
        print(x.date())
        MONTH = x.month
        YEAR = x.year
        print(MONTH, YEAR)
        sundays = 0
        cal = calendar.Calendar()

        for day in cal.itermonthdates(YEAR, MONTH):
            if day.weekday() == 6 and day.month == MONTH:
                sundays += 1
        # print(sundays)
        # print(monthrange(YEAR, MONTH))
        working_days = monthrange(YEAR, MONTH)[1] - sundays
        print('Working Days:', working_days)
        return working_days
            

    def format_ops_salary_export_data(self, section, final_df, department_level=0):
        if department_level == 1:
            final_df['temp'] = section
            final_df['Department -NEW'] = final_df[['temp','Department -NEW']].agg('--'.join, axis=1)
            final_df = final_df.drop('temp',axis = 1)
            final_df = (final_df.pivot_table(index=["unit_name","month"], columns=['Department -NEW'], values=[section]).reset_index())
            final_df.columns = final_df.columns.droplevel()

            final_df.rename(columns={final_df.columns[0]: 'unit_name'}, inplace = True)
            col_name_list = final_df.columns.tolist()
            col_name_list[1] = 'month'
            final_df.columns = col_name_list
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted {} Department Level Sheet.csv'.format(section)
        else:
            final_df['temp'] = section
            final_df['department_sub-department'] = final_df[['temp','Department -NEW', 'Sub-Department-NEW']].agg('--'.join, axis=1)
            final_df = final_df.drop('temp',axis = 1)
            final_df = final_df.drop(['Department -NEW', 'Sub-Department-NEW'], axis=1)

            final_df = (final_df.pivot_table(index=["unit_name","month"], columns=['department_sub-department'], values=[section]).reset_index())
            final_df.columns = final_df.columns.droplevel()

            final_df.rename(columns={final_df.columns[0]: 'unit_name'}, inplace = True)
            col_name_list = final_df.columns.tolist()
            col_name_list[1] = 'month'
            final_df.columns = col_name_list
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted {} Sub Department Level Sheet.csv'.format(section)
        final_df.to_csv(outputfile, index=False)

    
    def get_finance_mis_source(self):
        try:
            #Reading "MIS Source" excel file and creating list of sheet names
            inputfile = os.path.join(os.path.dirname(__file__))+'/mis/input_files/Finance MIS.xlsx'
            mis_source_excel = pd.ExcelFile(inputfile)
            mis_source_sheet_names = mis_source_excel.sheet_names
            unit_map_copy = unit_map[['Finance Mis', 'OPS MIS']]
            unit_map_copy.drop_duplicates(subset=['Finance Mis'], inplace=True)
            sheets_required = unit_map_copy.loc[:, 'Finance Mis'].values.tolist()

            today = datetime.today()
            current_month = datetime.today().month
            current_year = datetime.today().year

            if current_month in (1,2,3,4):
                start_year = current_year - 1
            else:
                start_year = current_year

            current_date = start_date = datetime(start_year, 4, 1)

            end_year = start_year + 1
            end_date = datetime(end_year, 3, 1)
            datelist = []
            while current_date <= end_date:
                datelist.append(str(current_date.date()))
                current_date = current_date + relativedelta(months=+1)

            mis_source_sheet_names = sheets_required
            cols = ['ref_ind','KPI']
            cols.extend(datelist)

            final_mis_source_df = pd.DataFrame()
            for j in range(len(mis_source_sheet_names)):
                # Reading single_sheet for the 'MIS_Source' file, converting column type to string, and reading particular columns into 'mis_source_sheet_df'
                mis_source_sheet_df = pd.read_excel(mis_source_excel, sheet_name = mis_source_sheet_names[j])

                mis_source_sheet_df['Unnamed: 1'] = mis_source_sheet_df['Unnamed: 1'].astype(str)
                mis_source_sheet_df = mis_source_sheet_df.iloc[:, [1,2,14,15,16,17,18,19,20,21,22,23,24,25]]
                
                mis_source_sheet_df.columns = cols
                mis_source_sheet_df = mis_source_sheet_df[mis_source_sheet_df['ref_ind'] != 'nan']
                
                #Taking Transpose of 'mis_source_df'
                mis_source_df = mis_source_sheet_df.transpose()

                #Setting row 'KPI' as column names
                mis_source_df.columns = mis_source_df.iloc[1]
                
                mis_source_df.drop(['ref_ind','KPI'], axis=0, inplace=True)

                #Renaming Columns
                mis_source_df.rename(columns = {'GROSS SALES':'Gross Sales (W/o Gst)',
                                                'Inter city Delivery Cost':'1.b Intercity cost (MIS)',
                                                'Net Revenue':'Net Revenue (after discuount W/o Gst)',
                                                'Salaries & Wages':'1. Salaries & Wages',
                                                'Purchase of Packaging Material - Polythene Bags':'3.a Polybag cost',
                                                'Purchase of Packaging Material - Others':'3.b other packing cost',
                                                'Electricity':'4. Electricity',
                                                'Telephone & Internet':'5. Telephone & Internet',
                                                'Commission expenses':'6. Commission expenses',
                                                'Warehouse Rent':'7. Warehouse Rent',
                                                'Cash Discounts to Retailers':'8. Cash Discounts to Retailers',
                                                'Other Operating Expenses':'9. Other Operating Expenses',
                                                'Printing & Stationery - Computer':'9.a. Printing Cost',
                                                'Printing & Stationery - Paper':'9.b. Paper Cost',
                                                'Printing & Stationery - Others':'9.c. other stationary Cost',
                                                'Other income':'10. Other income',
                                                'Expenses THREPSI Distribution':'THEA reimbursement',
                                                'Total Operating Expenses':'Total Operating Cost (after THEA reimbursement Deduction)',
                #                                  'Warehouse Rent':'1.b Intercity cost (MIS)',
                                                'Vehicle Running & Maintenance Cost':'1.a.ii Vehicle running & mainteinance cost (Actual)',
                                                'Professional Charges ':'Professional Charges',
        #                                          'Sales - Franchisee ' : 'Sales - Franchisee', 
                                                'Sales - Retail': 'dist_name'
                                                }, inplace = True)
                
                #Renaming NaN Column
                mis_source_df.columns = mis_source_df.columns.fillna('temp')

                #Creating Calculated Columns
                mis_source_df['Mid mile (MIS)'] = mis_source_df['1.b Intercity cost (MIS)']
                mis_source_df['Intercompany Sales (W/o Gst)'] = mis_source_df['Sales - Franchisee'] + mis_source_df['Sales - PE Market Place'] + mis_source_df['Sales - SupplyThis'] + mis_source_df['Sales to Aknamed'] + mis_source_df['Sales to MEDLIFE'] + mis_source_df['Sale to THREPSI'] + mis_source_df['Sales Inter Company Elimination - Cluster'] + mis_source_df['Sales Inter Company Elimination - Out of Cluster'] + mis_source_df['Sales Return - Intercompany']
                mis_source_df['Total Operating Cost (before THEA reimbursement Deduction)'] = mis_source_df['Total Operating Cost (after THEA reimbursement Deduction)'] + mis_source_df['THEA reimbursement']
                mis_source_df['3. Packaging Cost'] = mis_source_df['3.a Polybag cost'] + mis_source_df['3.b other packing cost']
                mis_source_df['Others'] = mis_source_df['AMC (Software)'] + mis_source_df['Audit Fees'] + mis_source_df['Bank Charges'] + mis_source_df['Books & Periodicals'] + mis_source_df['Computer Maintenance']+ mis_source_df['Generator Charges'] + mis_source_df['House Keeping'] + mis_source_df['Insurance Charges - Stock'] + mis_source_df['Legal Charges'] + mis_source_df['Loading & Unloading Charges'] + mis_source_df['Office Expenses'] + mis_source_df['Pooja Expenses'] + mis_source_df['Postage & Courier'] + mis_source_df['Professional Charges'] + mis_source_df['Rates & Taxes'] + mis_source_df['Rent - Guest House'] + mis_source_df['Repairs & Maintenance'] + mis_source_df['Sales Promotion'] + mis_source_df['Security Charges'] + mis_source_df['Travelling & Conveyance'] + mis_source_df['Vehicle Running & Maintenance'] + mis_source_df['Water Charges'] 
                # mis_source_df['1. Total Logistics Cost (MIS)'] = mis_source_df['6. Commission expenses'] + mis_source_df['7. Warehouse Rent']
                mis_source_df['dist_name'] = mis_source_sheet_names[j]
                

                if 'temp' in list(mis_source_df.columns):
                    mis_source_df['Last Mile (MIS)'] = mis_source_df['1.a.ii Vehicle running & mainteinance cost (Actual)'] + mis_source_df['Payment to outside Agency'] + mis_source_df['temp']
                elif('Dialhealth Delivery Cost' in list(mis_source_df.columns)):
                    mis_source_df['Last Mile (MIS)'] = mis_source_df['1.a.ii Vehicle running & mainteinance cost (Actual)'] + mis_source_df['Payment to outside Agency'] + mis_source_df['Dialhealth Delivery Cost']
                else:
                    mis_source_df['Last Mile (MIS)'] = mis_source_df['1.a.ii Vehicle running & mainteinance cost (Actual)'] + mis_source_df['Payment to outside Agency'] + mis_source_df['Other Delivery Charges']

                #Resetting Index
                mis_source_df.reset_index(inplace=True)
                mis_source_df.rename(columns={'index':'month','dist_name':'unit_name'}, inplace=True)
                
                mis_source_df['unit_name'].replace({'VPIM - JP':'VPIM','VPIM - PH':'VPIM'},inplace = True)

                mis_source_df = mis_source_df[['unit_name','month','Gross Sales (W/o Gst)','1.b Intercity cost (MIS)','Net Revenue (after discuount W/o Gst)',
                        '1. Salaries & Wages','Mid mile (MIS)','3.a Polybag cost','3.b other packing cost',
                        '4. Electricity','5. Telephone & Internet','6. Commission expenses','7. Warehouse Rent',
                        '8. Cash Discounts to Retailers','9. Other Operating Expenses','9.a. Printing Cost','9.b. Paper Cost',
                        '9.c. other stationary Cost','10. Other income','THEA reimbursement',
                        'Total Operating Cost (after THEA reimbursement Deduction)',
                        '1.a.ii Vehicle running & mainteinance cost (Actual)','Intercompany Sales (W/o Gst)',
                        'Total Operating Cost (before THEA reimbursement Deduction)','3. Packaging Cost','Others',
                        'Payment to outside Agency','Dialhealth Delivery Cost', 'Last Mile (MIS)']]

                final_mis_source_df = pd.concat([final_mis_source_df, mis_source_df], ignore_index = True)
            
            final_mis_source_df = final_mis_source_df.groupby(['unit_name','month']).sum()
            final_mis_source_df.reset_index(inplace=True)
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted Finance MIS Sheet.csv'
            final_mis_source_df.to_csv(outputfile, index=False)
            return final_mis_source_df
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            self.status = False
    
    def get_query_kpis(self):
        try:
            print('Getting Query Data...')
            query = sq.getQuery('mis_v2.sql', d, dest='vyuha/sql/')
            mis_data = sq.fetch4rmDL3(query)
            pan_india_kpi = mis_data.groupby(['dt', 'kpi_name'])['kpi_value'].sum().reset_index()
            pan_india_kpi.rename(columns={'kpi_value':'Pan India Value'}, inplace=True)
            tableau_source_df = mis_data.merge(pan_india_kpi, on=['kpi_name', 'dt'], how='left')

            tableau_source_df = tableau_source_df[tableau_source_df['dist_name'].notna()]
            tableau_source_df = (tableau_source_df.pivot_table(index=["dist_name","dt"], columns=['kpi_name'], values=['kpi_value']).reset_index())
            tableau_source_df.columns = tableau_source_df.columns.droplevel()

            tableau_source_df.rename(columns={tableau_source_df.columns[0]: 'unit_name'}, inplace = True)
            col_name_list = tableau_source_df.columns.tolist()
            col_name_list[1] = 'month'
            tableau_source_df.columns = col_name_list

            tableau_source_df.rename(columns = {'Inventory Value at 15th':'Inventory value - PTS/EPR (15th of the month)',
                                    'Inward Value':'Inward value ( PTS/EPR)',
                                    'Retail Billed Orders':'Billed orders ( Retail )',
                                    'Retail Picklist Line Items':'Billed line items (Retail)',
                                    'Retail Picklist Qty':'Billed Strips (Retail)',
                                    'Sale Return Value':'Sales Return ( At PTR after discount)',
                                    'Semi Billed Orders':'Billed orders (Inter company )',
                                    'Semi Picklist Line Items':'Billed line items (Inter company)',
                                    'Semi Picklist Qty':'Billed Strips (Inter company)',
                                    'Total Billed Orders':'Billed orders (Total)',
                                    'Total Picklist Line Items':'Billed line items (Total)',
                                    'Total Picklist Qty':'Billed Strips (Total)',
                                    'Sale Return Strips':'Sales Return Strips',
                                    'Godown to Store Strips':'Godown to store Strips'},inplace = True)

            unit_map_copy = unit_map[['Query', 'OPS MIS']]
            unit_map_copy.drop_duplicates(subset=['Query'], inplace=True)

            tableau_source_df = tableau_source_df.merge(unit_map_copy, left_on='unit_name', right_on='Query', how='left')
            tableau_source_df['unit_name'] = tableau_source_df['OPS MIS']
            tableau_source_df.drop(columns=['Query', 'OPS MIS'], inplace=True)
            
            tableau_source_df = tableau_source_df.groupby(['unit_name','month']).sum().reset_index()

            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted Query Data Sheet.csv'
            tableau_source_df.to_csv(outputfile, index=False)
            return tableau_source_df
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            self.status = False

    def logistics_last_mile_source(self):
        try:
            inputfile = os.path.join(os.path.dirname(__file__))+'/mis/input_files/Last Mile Logistics Cost Tracker.xlsx'
            logistics_source_excel = pd.ExcelFile(inputfile)
            
            current_month = datetime.today().month
            current_year = datetime.today().year

            if current_month in (1,2,3,4):
                start_year = current_year - 1
            else:
                start_year = current_year

            current_date = start_date = datetime(start_year, 4, 1)

            end_year = start_year + 1
            end_date = datetime(end_year, 3, 1)
            datelist = []
            while current_date <= end_date:
                datelist.append(current_date.date().strftime('%B %y'))
                current_date = current_date + relativedelta(months=+1)

            logistics_source_sheet_names = datelist.copy()
            print(logistics_source_sheet_names)

            
            master_logistics_df = pd.DataFrame()
            for sheet in logistics_source_sheet_names:
                try:
                    print("###### Reading sheet",sheet,"######")
                    logistics_source_sheet_df = pd.read_excel(logistics_source_excel, sheet_name = sheet)
                    logistics_source_df = logistics_source_sheet_df.iloc[[2,3,4,5,13,14,15], :].transpose()
                    logistics_source_df.columns = logistics_source_df.iloc[0]
                    logistics_source_df = logistics_source_df.iloc[1:]

                    logistics_source_df['month'] = datetime.strptime(sheet, '%B %y')
                    logistics_source_df = logistics_source_df.reset_index()
                    logistics_source_df.rename(columns = {'index':'unit_name'}, inplace = True)
                    logistics_source_df = logistics_source_df[logistics_source_df.unit_name.str.match('Unnamed.*')==False]
                    
                    logistics_source_df.rename(columns = {'Biker cost ':'Biker',
                                                        'Van Cost ':'Van',
                                                        'last Mile Total ':'1.a.i Payment to Outside Agency (Actual)',
                                                        'Supervisor_headcount':'3rd Supervisor Headcount',
                                                        'Biker_headcount':'3rd Biker Headcount',
                                                        'Van_headcount':'3rd Van Headcount'}, inplace = True)

                    master_logistics_df = pd.concat([master_logistics_df, logistics_source_df], ignore_index = True)
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno, str(e))
                    logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))

            unit_map_copy = unit_map[['Logistics', 'OPS MIS']]
            unit_map_copy.drop_duplicates(subset=['Logistics'], inplace=True)

            final_df = master_logistics_df.merge(unit_map_copy, left_on='unit_name', right_on='Logistics', how='left')
            final_df['unit_name'] = final_df['OPS MIS']
            final_df.drop(columns=['Logistics', 'OPS MIS'], inplace=True)
            
            final_df = final_df.groupby(['unit_name','month']).sum().reset_index()
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted Logistics Last Mile Sheet.csv'
            final_df.to_csv(outputfile, index=False)
            return final_df
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.status = False

    def logistics_mid_mile_source(self):
        try:
            #Reading "Logistics Source" excel file and creating list of sheet names
            inputfile = os.path.join(os.path.dirname(__file__))+'/mis/input_files/Mid Mile Logistics Cost Tracker.xlsx'

            logistics_source_sheet_df = pd.read_excel(inputfile)
            logistics_source_sheet_df = logistics_source_sheet_df.transpose()
            logistics_source_sheet_df = logistics_source_sheet_df.reset_index()
            unit_name_df = logistics_source_sheet_df['index'][2:]
            month_kpi_dict = {}
            for i in range(len(logistics_source_sheet_df.columns)-1):
                key = str(logistics_source_sheet_df[i].iloc[0])
                month_kpi_dict[key[0:10]]= pd.DataFrame(logistics_source_sheet_df[i].iloc[2:])
            month_name = list(month_kpi_dict.keys())

            final_mid_mile_df = pd.DataFrame()
            for i in range(len(month_name)):
                mid_mile_df = pd.DataFrame(columns = ['unit_name', 'month', 'Intercity cost (Actual)'])
                mid_mile_df['unit_name'] = unit_name_df
                mid_mile_df['month'] = month_name[i]
                mid_mile_df['Mid mile (Actual)'] = month_kpi_dict[month_name[i]]
                final_mid_mile_df = pd.concat([final_mid_mile_df,mid_mile_df], ignore_index = True)
            
            unit_map_copy = unit_map[['Logistics', 'OPS MIS']]
            unit_map_copy.drop_duplicates(subset=['Logistics'], inplace=True)
            
            final_df = final_mid_mile_df.merge(unit_map_copy, left_on='unit_name', right_on='Logistics', how='left')
            final_df['unit_name'] = final_df['OPS MIS']
            final_df.drop(columns=['Logistics', 'OPS MIS'], inplace=True)
            final_df =  final_df.groupby(['unit_name','month']).sum().reset_index()
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted Logistics Mid Mile Sheet.csv'
            final_df.to_csv(outputfile, index=False)

            return final_mid_mile_df
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.status = False


    def merge_sheets(self):
        try:
            sheets = os.listdir(os.path.join(os.path.dirname(__file__))+'/mis/output_files/')
            main_file = None
            for sheet in sheets:
                print('Sheet Name: {}'.format(sheet))
                try:
                    if 'Extracted' not in sheet:
                        continue

                    inputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/{}'.format(sheet)
                    df = pd.read_csv(inputfile)
                    if main_file is None:
                        main_file = df
                    else:
                        if 'warehouse' in sheet.lower():
                            main_file = main_file.merge(df,on=['unit_name'], how='outer')
                        else:
                            main_file = main_file.merge(df,on=['unit_name','month'], how='outer')
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno, str(e))
                    logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))

            # print(main_file.columns.tolist())
            main_file = self.calculate_kpis(main_file)


            # print(main_file.columns.tolist())
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/Merged Calculated Sheet.csv'
            main_file.to_csv(outputfile, index=False)
            self.generate_key_file(main_file)
            return main_file
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.status = False

    def calculate_kpis(self, all_kpis_df):
        try:
            print(type(all_kpis_df))
            # print(all_kpis_df.columns.tolist())
            # Logistics kpis
            all_kpis_df['Total Salary--Delivery'] = all_kpis_df['Total Salary--Delivery'] / 1000000
            all_kpis_df['Supervisor'] = all_kpis_df['Supervisor'] / 1000000
            all_kpis_df['Biker'] = all_kpis_df['Biker'] / 1000000
            all_kpis_df['Van'] = all_kpis_df['Van'] / 1000000
            all_kpis_df['1.a.i Payment to Outside Agency (Actual)'] = all_kpis_df['1.a.i Payment to Outside Agency (Actual)'] / 1000000

            all_kpis_df['Inhouse Bikers Cost'] = all_kpis_df['Total Salary--Delivery']
            
            # Replacing NaN with '0'
            all_kpis_df[['Last Mile (MIS)','Inhouse Bikers Cost','1.a.i Payment to Outside Agency (Actual)','Payment to outside Agency']] = all_kpis_df[['Last Mile (MIS)','Inhouse Bikers Cost','1.a.i Payment to Outside Agency (Actual)','Payment to outside Agency']].replace(np.nan,0)
            all_kpis_df['Total Last Mile (MIS + Inhouse Delivery)'] = all_kpis_df['Last Mile (MIS)'] + all_kpis_df['Inhouse Bikers Cost']
            all_kpis_df['Total Last mile (Actual)'] = all_kpis_df['1.a.i Payment to Outside Agency (Actual)']
            all_kpis_df['Provision Mid mile'] = all_kpis_df['Mid mile (MIS)'] - all_kpis_df['Mid mile (Actual)']
            all_kpis_df['Provision Last mile Payment to ourside agency'] = all_kpis_df['Payment to outside Agency'] - all_kpis_df['1.a.i Payment to Outside Agency (Actual)']


            all_kpis_df['1.b Delivery Salary % of Net Revenue'] = all_kpis_df['Total Salary--Delivery']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Total Last mile (Actual) as a % of Net Revenue'] = all_kpis_df['Total Last mile (Actual)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Mid mile (MIS) as a % of Net Revenue'] = all_kpis_df['Mid mile (MIS)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Last Mile (MIS) as a % of Net Revenue'] = all_kpis_df['Last Mile (MIS)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Total Last Mile (MIS + Inhouse Delivery) as a % of Net Revenue'] = (all_kpis_df['Last Mile (MIS)'] + all_kpis_df['Inhouse Bikers Cost'])/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Mid mile (Actual) as a % of Net Revenue'] = all_kpis_df['Mid mile (Actual)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            
            all_kpis_df['1.b Delivery Salary cost per order'] = all_kpis_df['Total Salary--Delivery']/all_kpis_df['Billed orders (Total)']*1000000

            ##########
            all_kpis_df['Avg Order Value (Gross)'] = all_kpis_df['Gross Sales (W/o Gst)']*1000000/all_kpis_df['Billed orders (Total)']
            all_kpis_df['Avg Order Value (retail)'] = all_kpis_df['Net Revenue (after discuount W/o Gst)']*1000000/all_kpis_df['Billed orders ( Retail )']
            
            #converted to millions
            all_kpis_df['Mid mile (Actual)'] = all_kpis_df['Mid mile (Actual)']/1000000
            # all_kpis_df['1. Salaries & Wages'] = all_kpis_df['1. Salaries & Wages'] / 1000000
            all_kpis_df['Total Salary--Inward'] = all_kpis_df['Total Salary--Inward'] / 1000000
            all_kpis_df['Total Salary--Store'] = all_kpis_df['Total Salary--Store'] / 1000000
            all_kpis_df['Total Salary--Checking'] = all_kpis_df['Total Salary--Checking'] / 1000000
            all_kpis_df['Total Salary--Dispatch'] = all_kpis_df['Total Salary--Dispatch'] / 1000000
            all_kpis_df['Total Salary--Audit & Refilling'] = all_kpis_df['Total Salary--Audit & Refilling'] / 1000000
            all_kpis_df['Total Salary--Expiry'] = all_kpis_df['Total Salary--Expiry'] / 1000000
            all_kpis_df['Total Salary--Sales Return'] = all_kpis_df['Total Salary--Sales Return'] / 1000000
            all_kpis_df['Total Salary--Overall Operation'] = all_kpis_df['Total Salary--Overall Operation'] / 1000000
            all_kpis_df['Total Salary--Admin'] = all_kpis_df['Total Salary--Admin'] / 1000000
            all_kpis_df['Total Salary--Finance & Accounts'] = all_kpis_df['Total Salary--Finance & Accounts'] / 1000000
            all_kpis_df['Total Salary--Purchase'] = all_kpis_df['Total Salary--Purchase'] / 1000000
            all_kpis_df['Total Salary--Sales'] = all_kpis_df['Total Salary--Sales'] / 1000000
            all_kpis_df['Total Salary--IT'] = all_kpis_df['Total Salary--IT'] / 1000000
            all_kpis_df['Total Salary--HR'] = all_kpis_df['Total Salary--HR'] / 1000000
            all_kpis_df['Total Salary--Data analyst'] = all_kpis_df['Total Salary--Data analyst'] / 1000000
            all_kpis_df['Total Salary--Overall Non - Ops'] = all_kpis_df['Total Salary--Overall Non - Ops'] / 1000000
                        


            all_kpis_df['Total Operating Cost as a % of Net Revenue (before THEA reimbursement Deduction)'] = all_kpis_df['Total Operating Cost (before THEA reimbursement Deduction)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.A Salaries & Wages as a % of Net Revenue'] = all_kpis_df['1. Salaries & Wages']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            
            # all_kpis_df['2. Total Delivery Cost'] = all_kpis_df['2.a Delivery Cost']/all_kpis_df['2.b Intercity (Mid Mile)']
            # all_kpis_df['2A. Delivery Cost as a % of Net Revenue'] = all_kpis_df['2. Total Delivery Cost']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Delivery cost (Swifto, 3rd party)'] = all_kpis_df['']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Rent'] = all_kpis_df['']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Expansion & Sales'] = all_kpis_df['']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Vehicle running & mainteinance cost'] = all_kpis_df['']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['3.A. Packaging Cost as a % of Net Revenue'] = all_kpis_df['3. Packaging Cost']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['4.A. Electricity as a % of Net Revenue'] = all_kpis_df['4. Electricity']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['5.A. Telephone & Internet as a % of Net Revenue'] = all_kpis_df['5. Telephone & Internet']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['6.A. Commission expenses as a % of Net Revenue'] = all_kpis_df['6. Commission expenses']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['7.A. Warehouse Rent as a % of Net Revenue'] = all_kpis_df['7. Warehouse Rent']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['8.A. Cash Discounts to Retailers as a % of Net Revenue'] = all_kpis_df['8. Cash Discounts to Retailers']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['9.A. Other Operating Expenses as a % of Net Revenue'] = all_kpis_df['9. Other Operating Expenses']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['10.A. Other income as a % of Net Revenue'] = all_kpis_df['10. Other income']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['THEA Reimbursemnt as a % of Net Revenue'] = all_kpis_df['THEA reimbursement']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Total Operating Cost as a % of Net Revenue (After THEA reimbursement Deduction)'] = all_kpis_df['Total Operating Cost (after THEA reimbursement Deduction)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df[''] = all_kpis_df['1. Salaries & Wages']/all_kpis_df['']
            # all_kpis_df['Cost per Mandays - Salary & Wages'] = all_kpis_df['1. Salaries & Wages']/all_kpis_df['Worked Mandays']
            # all_kpis_df['Cost per ManMonth - Salary & Wages'] = all_kpis_df['']/all_kpis_df['']
            all_kpis_df['Inward Productivity (Inward Strips Per Manday)'] = all_kpis_df['Inward Strips']/all_kpis_df['Worked Mandays--Present Head Count--Inward']
            all_kpis_df['Store Productivity (Billed line Items per Manday)'] = all_kpis_df['Billed line items (Total)']/all_kpis_df['Worked Mandays--Present Head Count--Store']
            all_kpis_df['Checking Productivity (Billed Strips Per Manday)'] = all_kpis_df['Billed Strips (Total)']/all_kpis_df['Worked Mandays--Present Head Count--Checking']
            all_kpis_df['Dispatch Productivity (Billed Orders per Manday)'] = all_kpis_df['Billed orders (Total)']/all_kpis_df['Worked Mandays--Present Head Count--Dispatch']
            all_kpis_df['Audit and refilling Productivity (Godown to Store Strips per Manday)'] = all_kpis_df['Godown to store Strips']/all_kpis_df['Worked Mandays--Present Head Count--Audit & Refilling']
            all_kpis_df['Expiry Productivity (Expiry Return Strips Per Manday)'] = all_kpis_df['Expiry Return Strips']/all_kpis_df['Worked Mandays--Present Head Count--Expiry']
            all_kpis_df['Sales return Productivity ( Sales Return Strips Per Manday)'] = all_kpis_df['Sales Return Strips']/all_kpis_df['Worked Mandays--Present Head Count--Sales Return']
            all_kpis_df['Purchase Productivity ( Inward Value per Manday)'] = all_kpis_df['Inward value ( PTS/EPR)']/all_kpis_df['Worked Mandays--Present Head Count--Purchase']
            all_kpis_df['Sales Productivity (Net Revenue Per Manday)'] = all_kpis_df['Net Revenue (after discuount W/o Gst)']/all_kpis_df['Worked Mandays--Present Head Count--Sales']
            all_kpis_df['Inventory per sqft'] = all_kpis_df['Inventory value - PTS/EPR (15th of the month)']/all_kpis_df['Warehouse area']
            all_kpis_df['Net Revenue per sqft'] = all_kpis_df['Net Revenue (after discuount W/o Gst)'] * 1000000/all_kpis_df['Warehouse area']

            all_kpis_df['1. Salaries & Wages cost per order'] = all_kpis_df['1. Salaries & Wages']/all_kpis_df['Billed orders (Total)']*1000000
            # all_kpis_df['2. Delivery Cost per order'] = all_kpis_df['2. Total Delivery Cost']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['3. Packaging Cost per order'] = all_kpis_df['3. Packaging Cost']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['4. Electricity per order'] = all_kpis_df['4. Electricity']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['5. Telephone & Internet per order'] = all_kpis_df['5. Telephone & Internet']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['6. Commission expenses per order'] = all_kpis_df['6. Commission expenses']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['7. Warehouse Rent per order'] = all_kpis_df['7. Warehouse Rent']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['8. Cash Discounts to Retailers per order'] = all_kpis_df['8. Cash Discounts to Retailers']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['9. Other Operating Expenses per order'] = all_kpis_df['9. Other Operating Expenses']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['10. Other income per order'] = all_kpis_df['10. Other income']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['THEA reimbursement per order'] = all_kpis_df['THEA reimbursement']/all_kpis_df['Billed orders (Total)']*1000000
            # all_kpis_df['Total Operating Cost (after THEA reimbursement Deduction)'] = all_kpis_df['Total Operating Cost (after THEA reimbursement Deduction)']/all_kpis_df['Billed orders (Total)']
            
            all_kpis_df['1.a.i Inward cost per order'] = all_kpis_df['Total Salary--Inward']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.ii Store cost per order'] = all_kpis_df['Total Salary--Store']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.iii Checking cost per order'] = all_kpis_df['Total Salary--Checking']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.iv Dispatch cost per order'] = all_kpis_df['Total Salary--Dispatch']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.v Audit and refilling cost per order'] = all_kpis_df['Total Salary--Audit & Refilling']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.vi Expiry cost per order'] = all_kpis_df['Total Salary--Expiry']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.vii Sales return cost per order'] = all_kpis_df['Total Salary--Sales Return']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.a.viii Overall Operations cost per order'] = all_kpis_df['Total Salary--Overall Operation']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.d Admin cost per order'] = all_kpis_df['Total Salary--Admin']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.e Finance & Accounts cost per order'] = all_kpis_df['Total Salary--Finance & Accounts']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.f Purchase cost per order'] = all_kpis_df['Total Salary--Purchase']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.g Sales cost per order'] = all_kpis_df['Total Salary--Sales']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.h IT cost per order'] = all_kpis_df['Total Salary--IT']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.i HR cost per order'] = all_kpis_df['Total Salary--HR']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.j Data Analyst cost per order'] = all_kpis_df['Total Salary--Data analyst']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['1.k Overall Non - Operations cost per order'] = all_kpis_df['Total Salary--Overall Non - Ops']/all_kpis_df['Billed orders (Total)']*1000000

            all_kpis_df['3.a Polybag cost cost per order'] = all_kpis_df['3.a Polybag cost']/all_kpis_df['Billed orders (Total)']*1000000
            all_kpis_df['3.b other packing cost cost per order'] = all_kpis_df['3.b other packing cost']/all_kpis_df['Billed orders (Total)']*1000000


            all_kpis_df['1.a.i Inward % of Net Revenue'] = all_kpis_df['Total Salary--Inward']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.ii Store cost per order % of Net Revenue'] = all_kpis_df['Total Salary--Store']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.iii Checking % of Net Revenue'] = all_kpis_df['Total Salary--Checking']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.iv Dispatch % of Net Revenue'] = all_kpis_df['Total Salary--Dispatch']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.v Audit and refilling % of Net Revenue'] = all_kpis_df['Total Salary--Audit & Refilling']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.vi Expiry % of Net Revenue'] = all_kpis_df['Total Salary--Expiry']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.vii Sales return % of Net Revenue'] = all_kpis_df['Total Salary--Sales Return']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.a.viii Overall Operations % of Net Revenue'] = all_kpis_df['Total Salary--Overall Operation']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.d Admin % of Net Revenue'] = all_kpis_df['Total Salary--Admin']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.e Finance & Accounts % of Net Revenue'] = all_kpis_df['Total Salary--Finance & Accounts']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.f Purchase % of Net Revenue'] = all_kpis_df['Total Salary--Purchase']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.g Sales % of Net Revenue'] = all_kpis_df['Total Salary--Sales']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.h IT % of Net Revenue'] = all_kpis_df['Total Salary--IT']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.i HR % of Net Revenue'] = all_kpis_df['Total Salary--HR']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.j Data Analyst % of Net Revenue'] = all_kpis_df['Total Salary--Data analyst']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['1.k Overall Non - Operations % of Net Revenue'] = all_kpis_df['Total Salary--Overall Non - Ops']/all_kpis_df['Net Revenue (after discuount W/o Gst)']


            all_kpis_df['3.a Polybag cost Cost as a % of Net Revenue'] = all_kpis_df['3.a Polybag cost']/all_kpis_df['Billed orders (Total)']
            all_kpis_df['3.b other packing cost Cost as a % of Net Revenue'] = all_kpis_df['3.b other packing cost']/all_kpis_df['Billed orders (Total)']

            
            # all_kpis_df['Vehicle running & mainteinance cost'] = all_kpis_df['1.a.ii Vehicle running & mainteinance cost (Actual)']
            # all_kpis_df['2. Total Logistics Cost (Actual)'] = all_kpis_df['Inhouse Bikers Cost'] + all_kpis_df['Vehicle running & mainteinance cost']
            # all_kpis_df['1.Total Logistics Cost (MIS) as a % of Net Revenue'] = all_kpis_df['2. Total Logistics Cost (Actual)'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['1.a Delivery Cost (MIS) as a % of Net Revenue'] = all_kpis_df['1.a Delivery Cost (Mis)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['1.b Intercity cost (MIS) as a % of Net Revenue'] = all_kpis_df['1.b Intercity cost (MIS)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['2.Total Logistics Cost (Actual) as a % of Net Revenue'] = all_kpis_df['2. Total Logistics Cost (Actual)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            
            all_kpis_df['Cost per man per month Inward'] = all_kpis_df['Total Salary--Inward'] / all_kpis_df['Present Head Count--Inward']
            all_kpis_df['Cost per man per month Store'] = all_kpis_df['Total Salary--Store'] / all_kpis_df['Present Head Count--Store']
            all_kpis_df['Cost per man per month Checking'] = all_kpis_df['Total Salary--Checking'] / all_kpis_df['Present Head Count--Checking']
            all_kpis_df['Cost per man per month Dispatch'] = all_kpis_df['Total Salary--Dispatch'] / all_kpis_df['Present Head Count--Dispatch']
            all_kpis_df['Cost per man per month Audit and refilling'] = all_kpis_df['Total Salary--Audit & Refilling'] / all_kpis_df['Present Head Count--Audit & Refilling']
            all_kpis_df['Cost per man per month Expiry'] = all_kpis_df['Total Salary--Expiry'] / all_kpis_df['Present Head Count--Expiry']
            all_kpis_df['Cost per man per month Sales return'] = all_kpis_df['Total Salary--Sales Return'] / all_kpis_df['Present Head Count--Sales Return']
            all_kpis_df['Cost per man per month Overall operations'] = all_kpis_df['Total Salary--Overall Operation'] / all_kpis_df['Present Head Count--Overall Operation']
            all_kpis_df['Cost per man per month Total Operations'] = all_kpis_df['Cost per man per month Inward'] + all_kpis_df['Cost per man per month Store'] + all_kpis_df['Cost per man per month Checking'] + all_kpis_df['Cost per man per month Dispatch'] + all_kpis_df['Cost per man per month Audit and refilling'] + all_kpis_df['Cost per man per month Expiry'] + all_kpis_df['Cost per man per month Sales return'] + all_kpis_df['Cost per man per month Overall operations']
            
            all_kpis_df['Cost per man per month Delivery'] = all_kpis_df['Total Salary--Delivery'] / all_kpis_df['Present Head Count--Delivery']
            all_kpis_df['Cost per man per month Admin'] = all_kpis_df['Total Salary--Admin'] / all_kpis_df['Present Head Count--Admin']
            all_kpis_df['Cost per man per month Finance & Accounts'] = all_kpis_df['Total Salary--Finance & Accounts'] / all_kpis_df['Present Head Count--Finance & Accounts']
            all_kpis_df['Cost per man per month Purchase'] = all_kpis_df['Total Salary--Purchase'] / all_kpis_df['Present Head Count--Purchase']
            all_kpis_df['Cost per man per month Sales'] = all_kpis_df['Total Salary--Sales'] / all_kpis_df['Present Head Count--Sales']
            all_kpis_df['Cost per man per month IT'] = all_kpis_df['Total Salary--IT'] / all_kpis_df['Present Head Count--IT']
            all_kpis_df['Cost per man per month HR'] = all_kpis_df['Total Salary--HR'] / all_kpis_df['Present Head Count--HR']
            all_kpis_df['Cost per man per month Data Analyst'] = all_kpis_df['Total Salary--Data analyst'] / all_kpis_df['Present Head Count--Data analyst']
            all_kpis_df['Cost per man per month Overall Non-Operations'] = all_kpis_df['Total Salary--Overall Non - Ops'] / all_kpis_df['Present Head Count--Overall Non - Ops']
            all_kpis_df['Total Biker headcount'] = (all_kpis_df['Present Head Count--Delivery'] + all_kpis_df['3rd Biker Headcount'])+(3 * all_kpis_df['3rd Van Headcount'])
            all_kpis_df['Cost Per Biker Per Month'] = (all_kpis_df['Biker'] + (all_kpis_df['Van'])*3) / all_kpis_df['Total Biker headcount']
            all_kpis_df['Sales Return ( At PTR after discount)'] = all_kpis_df['Sales Return ( At PTR after discount)'] / 1000000
            # all_kpis_df['Inward value ( PTS/EPR)'] = all_kpis_df['Inward value ( PTS/EPR)'] / 1000000
            all_kpis_df['Inventory value - PTS/EPR (15th of the month)'] = all_kpis_df['Inventory value - PTS/EPR (15th of the month)'] / 1000000
            all_kpis_df['Sale Return Value from customer'] = all_kpis_df['Sale Return Value from customer'] / 1000000
            all_kpis_df['Expiry Return Value from customer'] = all_kpis_df['Expiry Return Value from customer'] / 1000000
            all_kpis_df['Intransit breakage (Retail)'] = all_kpis_df['Intransit breakage (Retail)'] / 1000000
            all_kpis_df['Expiry from godown'] = all_kpis_df['Expiry from godown'] / 1000000
            all_kpis_df['Gowdown breakage'] = all_kpis_df['Gowdown breakage'] / 1000000
            all_kpis_df['Expiry to supplier'] = all_kpis_df['Expiry to supplier'] / 1000000
            all_kpis_df['Value of Non-moving item'] = all_kpis_df['Value of Non-moving item'] / 1000000
            all_kpis_df['Sale Return Percentage'] = all_kpis_df['Sale Return Value from customer'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
            all_kpis_df['Expiry Return Percentage'] = all_kpis_df['Expiry Return Value from customer'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Delivery Cost Provision'] = all_kpis_df['1.a Delivery Cost (Mis)'] - (all_kpis_df['1.a.i Payment to Outside Agency (Actual)'] + all_kpis_df['1.a.ii Vehicle running & mainteinance cost (Actual)'])
            # all_kpis_df['Intercity Provision'] = all_kpis_df['1.b Intercity cost (MIS)'] - all_kpis_df['Mid mile (Actual)']
            # all_kpis_df['Delivery Cost % Net Rev Provision'] = all_kpis_df['Delivery Cost Provision'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Intercity % Net Rev Provision'] = all_kpis_df['Intercity Provision'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
            # all_kpis_df['Fill rate %'] = all_kpis_df['Gross Sales (W/o Gst)'] / (all_kpis_df['Gross Sales (W/o Gst)'] - all_kpis_df['Lost Sales'])
            all_kpis_df['Percentage of Non-moving item'] = all_kpis_df['Value of Non-moving item'] / all_kpis_df['Inventory value - PTS/EPR (15th of the month)']
            
            #Replacing NaN with '0'
            all_kpis_df[['Total Salary--Inward','Total Salary--Store','Total Salary--Checking','Total Salary--Dispatch','Total Salary--Audit & Refilling','Total Salary--Expiry','Total Salary--Sales Return','Total Salary--Overall Operation']] = all_kpis_df[['Total Salary--Inward','Total Salary--Store','Total Salary--Checking','Total Salary--Dispatch','Total Salary--Audit & Refilling','Total Salary--Expiry','Total Salary--Sales Return','Total Salary--Overall Operation']].replace(np.nan,0) 
            all_kpis_df['Ops Salaries & Wages'] = all_kpis_df['Total Salary--Inward'] + all_kpis_df['Total Salary--Store'] + all_kpis_df['Total Salary--Checking'] + all_kpis_df['Total Salary--Dispatch'] + all_kpis_df['Total Salary--Audit & Refilling'] + all_kpis_df['Total Salary--Expiry'] + all_kpis_df['Total Salary--Sales Return'] + all_kpis_df['Total Salary--Overall Operation']
            all_kpis_df['Ops Salaries & Wages as a % of Net Revenue'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
            
            #Replacing NaN with '0'
            all_kpis_df[['Present Head Count--Inward', 'Present Head Count--Store', 'Present Head Count--Checking', 'Present Head Count--Dispatch', 'Present Head Count--Audit & Refilling', 'Present Head Count--Expiry', 'Present Head Count--Sales Return', 'Present Head Count--Overall Operation']] = all_kpis_df[['Present Head Count--Inward', 'Present Head Count--Store', 'Present Head Count--Checking', 'Present Head Count--Dispatch', 'Present Head Count--Audit & Refilling', 'Present Head Count--Expiry', 'Present Head Count--Sales Return', 'Present Head Count--Overall Operation']].replace(np.nan,0)
            all_kpis_df['Present Head Count Ops'] = all_kpis_df['Present Head Count--Inward'] + all_kpis_df['Present Head Count--Store'] + all_kpis_df['Present Head Count--Checking'] + all_kpis_df['Present Head Count--Dispatch'] + all_kpis_df['Present Head Count--Audit & Refilling'] + all_kpis_df['Present Head Count--Expiry'] + all_kpis_df['Present Head Count--Sales Return'] + all_kpis_df['Present Head Count--Overall Operation']
            all_kpis_df['Cost per man per month Total Operations'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Present Head Count Ops']
            all_kpis_df['Salary and wages cost per order'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Billed orders (Total)']*1000000
            

            ### Calculating Percentage KPI's headers
            # all_kpis_df['1.A Salaries & Wages as a % of Net Revenue_head'] = 

            #Renaming kpi's
            all_kpis_df.rename(columns = {'Working Days--Worked Mandays--Present Head Count--Admin':'Worked Mandays--Admin',
                                        'Working Days--Worked Mandays--Present Head Count--Audit & Refilling':'Worked Mandays--Audit & Refilling',
                                        'Working Days--Worked Mandays--Present Head Count--Checking':'Worked Mandays--Checking',
                                        'Working Days--Worked Mandays--Present Head Count--Data analyst':'Worked Mandays--Data analyst',
                                        'Working Days--Worked Mandays--Present Head Count--Delivery':'Worked Mandays--Delivery',
                                        'Working Days--Worked Mandays--Present Head Count--Dispatch':'Worked Mandays--Dispatch',
                                        'Working Days--Worked Mandays--Present Head Count--Expiry':'Worked Mandays--Expiry',
                                        'Working Days--Worked Mandays--Present Head Count--Finance & Accounts':'Worked Mandays--Finance & Accounts',
                                        'Working Days--Worked Mandays--Present Head Count--HR':'Worked Mandays--HR',
                                        'Working Days--Worked Mandays--Present Head Count--IT':'Worked Mandays--IT',
                                        'Working Days--Worked Mandays--Present Head Count--Inward':'Worked Mandays--Inward',
                                        'Working Days--Worked Mandays--Present Head Count--Overall Non - Ops':'Worked Mandays--Overall Non - Ops',
                                        'Working Days--Worked Mandays--Present Head Count--Overall Operation':'Worked Mandays--Overall Operation',
                                        'Working Days--Worked Mandays--Present Head Count--Purchase':'Worked Mandays--Purchase',
                                        'Working Days--Worked Mandays--Present Head Count--Sales':'Worked Mandays--Sales',
                                        'Working Days--Worked Mandays--Present Head Count--Sales Return':'Worked Mandays--Sales Return',
                                        'Working Days--Worked Mandays--Present Head Count--Store':'Worked Mandays--Store'}, inplace = True)

            all_kpis_df = all_kpis_df[['unit_name','month','Gross Sales (W/o Gst)','Intercompany Sales (W/o Gst)','Net Revenue (after discuount W/o Gst)','Avg Order Value (Gross)',
                    'Avg Order Value (retail)','Billed orders (Total)','Billed orders (Inter company )','Billed orders ( Retail )',
                    'Billed line items (Total)','Billed line items (Inter company)','Billed line items (Retail)','Billed Strips (Total)',
                    'Billed Strips (Inter company)','Billed Strips (Retail)','Inward Strips','Godown to store Strips','Expiry Return Strips',
                    'Sales Return Strips','Sales Return ( At PTR after discount)','Inward value ( PTS/EPR)',
                    'Inventory value - PTS/EPR (15th of the month)','Warehouse area','Total Operating Cost (before THEA reimbursement Deduction)',
                    '1. Salaries & Wages','Total Salary--Inward','Total Salary--Store','Total Salary--Checking','Total Salary--Dispatch',
                    'Total Salary--Audit & Refilling','Total Salary--Expiry','Total Salary--Sales Return','Total Salary--Overall Operation',
                    'Total Salary--Delivery','Total Salary--Admin','Total Salary--Finance & Accounts','Total Salary--Purchase','Total Salary--Sales',
                    'Total Salary--IT','Total Salary--HR','Total Salary--Data analyst','Total Salary--Overall Non - Ops',
                    'Last Mile (MIS)','Mid mile (MIS)','Mid mile (Actual)','3. Packaging Cost','3.a Polybag cost',
                    '3.b other packing cost','4. Electricity','5. Telephone & Internet','6. Commission expenses','7. Warehouse Rent',
                    '8. Cash Discounts to Retailers','9. Other Operating Expenses','9.a. Printing Cost','9.b. Paper Cost','9.c. other stationary Cost',
                    'Others','10. Other income','THEA reimbursement','Total Operating Cost (after THEA reimbursement Deduction)',
                    'Total Operating Cost as a % of Net Revenue (before THEA reimbursement Deduction)','1.A Salaries & Wages as a % of Net Revenue',
                    '1.a.i Inward % of Net Revenue','1.a.ii Store cost per order % of Net Revenue','1.a.iii Checking % of Net Revenue',
                    '1.a.iv Dispatch % of Net Revenue','1.a.v Audit and refilling % of Net Revenue','1.a.vi Expiry % of Net Revenue',
                    '1.a.vii Sales return % of Net Revenue','1.a.viii Overall Operations % of Net Revenue','1.b Delivery Salary % of Net Revenue',
                    '1.d Admin % of Net Revenue','1.e Finance & Accounts % of Net Revenue','1.f Purchase % of Net Revenue','1.g Sales % of Net Revenue',
                    '1.h IT % of Net Revenue','1.i HR % of Net Revenue','1.j Data Analyst % of Net Revenue','1.k Overall Non - Operations % of Net Revenue',
                     'Last Mile (MIS) as a % of Net Revenue','Mid mile (MIS) as a % of Net Revenue',
                    '3.A. Packaging Cost as a % of Net Revenue','3.a Polybag cost Cost as a % of Net Revenue','3.b other packing cost Cost as a % of Net Revenue',
                    '4.A. Electricity as a % of Net Revenue','5.A. Telephone & Internet as a % of Net Revenue',
                    '6.A. Commission expenses as a % of Net Revenue','7.A. Warehouse Rent as a % of Net Revenue',
                    '8.A. Cash Discounts to Retailers as a % of Net Revenue','9.A. Other Operating Expenses as a % of Net Revenue',
                    '10.A. Other income as a % of Net Revenue','THEA Reimbursemnt as a % of Net Revenue',
                    'Total Operating Cost as a % of Net Revenue (After THEA reimbursement Deduction)','Inward Productivity (Inward Strips Per Manday)',
                    'Store Productivity (Billed line Items per Manday)','Checking Productivity (Billed Strips Per Manday)',
                    'Dispatch Productivity (Billed Orders per Manday)','Audit and refilling Productivity (Godown to Store Strips per Manday)',
                    'Expiry Productivity (Expiry Return Strips Per Manday)','Sales return Productivity ( Sales Return Strips Per Manday)',
                    'Purchase Productivity ( Inward Value per Manday)','Sales Productivity (Net Revenue Per Manday)','Inventory per sqft',
                    'Net Revenue per sqft','1.b Intercity cost (MIS)',
                    '1.a.i Payment to Outside Agency (Actual)','Supervisor','Biker','Van','1.a.ii Vehicle running & mainteinance cost (Actual)','1. Salaries & Wages cost per order',
                    '1.a.i Inward cost per order','1.a.ii Store cost per order','1.a.iii Checking cost per order','1.a.iv Dispatch cost per order',
                    '1.a.v Audit and refilling cost per order','1.a.vi Expiry cost per order','1.a.vii Sales return cost per order',
                    '1.a.viii Overall Operations cost per order','1.b Delivery Salary cost per order','1.d Admin cost per order',
                    '1.e Finance & Accounts cost per order','1.f Purchase cost per order','1.g Sales cost per order','1.h IT cost per order',
                    '1.i HR cost per order','1.j Data Analyst cost per order','1.k Overall Non - Operations cost per order',
                    '3. Packaging Cost per order','3.a Polybag cost cost per order','3.b other packing cost cost per order','4. Electricity per order',
                    '5. Telephone & Internet per order','6. Commission expenses per order','7. Warehouse Rent per order','8. Cash Discounts to Retailers per order',
                    '9. Other Operating Expenses per order','10. Other income per order','THEA reimbursement per order',
                    'Inhouse Bikers Cost','Total Last Mile (MIS + Inhouse Delivery)',
                    'Total Last Mile (MIS + Inhouse Delivery) as a % of Net Revenue',
                    'Mid mile (Actual) as a % of Net Revenue',
                    'Present Head Count--Inward','Present Head Count--Store','Present Head Count--Checking','Present Head Count--Dispatch',
                    'Present Head Count--Audit & Refilling','Present Head Count--Expiry','Present Head Count--Sales Return',
                    'Present Head Count--Overall Operation','Present Head Count--Delivery','Present Head Count--Admin',
                    'Present Head Count--Finance & Accounts','Present Head Count--Purchase','Present Head Count--Sales',
                    'Present Head Count--IT','Present Head Count--HR','Present Head Count--Data analyst','Present Head Count--Overall Non - Ops',
                    'Worked Mandays--Inward','Worked Mandays--Store','Worked Mandays--Checking','Worked Mandays--Dispatch',
                    'Worked Mandays--Audit & Refilling','Worked Mandays--Expiry','Worked Mandays--Sales Return','Worked Mandays--Overall Operation',
                    'Worked Mandays--Delivery','Worked Mandays--Admin','Worked Mandays--Finance & Accounts','Worked Mandays--Purchase',
                    'Worked Mandays--Sales','Worked Mandays--IT','Worked Mandays--HR','Worked Mandays--Data analyst',
                    'Worked Mandays--Overall Non - Ops','Cost per man per month Inward','Cost per man per month Store','Cost per man per month Checking',
                    'Cost per man per month Dispatch', 'Cost per man per month Audit and refilling', 'Cost per man per month Expiry',
                    'Cost per man per month Sales return','Cost per man per month Overall operations','Cost per man per month Delivery',
                    'Cost per man per month Admin', 'Cost per man per month Finance & Accounts','Cost per man per month Purchase',
                    'Cost per man per month Sales','Cost per man per month IT','Cost per man per month HR','Cost per man per month Data Analyst',
                    'Cost per man per month Overall Non-Operations','3rd Supervisor Headcount','3rd Biker Headcount','3rd Van Headcount',
                    'Total Biker headcount','Cost Per Biker Per Month','Sale Return Value from customer','Expiry Return Value from customer',
                    'Intransit breakage (Retail)','Expiry from godown','Gowdown breakage','Expiry to supplier','Expiry return strip to supplier',
                    'Value of Non-moving item', 'Percentage of Non-moving item',
                    'Sale Return Percentage','Expiry Return Percentage','Total Last mile (Actual)','Payment to outside Agency','Dialhealth Delivery Cost',
                    'Total Last mile (Actual) as a % of Net Revenue','Provision Mid mile','Provision Last mile Payment to ourside agency','Ops Salaries & Wages',
                    'Ops Salaries & Wages as a % of Net Revenue','Present Head Count Ops','Cost per man per month Total Operations']]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            self.status = False
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
        return all_kpis_df
    
    def generate_key_file(self, final_df):
        try:
            kpi_names = final_df.columns.tolist()
            kpi_names.remove('unit_name')
            kpi_names.remove('month')
            final_df = pd.melt(final_df, id_vars=['unit_name','month'], value_vars=kpi_names)
            final_df.rename(columns = {'variable':'kpi_name','value':'kpi_value'}, inplace = True)
            final_df['key'] = final_df[['kpi_name','unit_name']].agg('_'.join, axis=1)
            final_df = final_df.fillna(0)
            # final_df['month'] = pd.to_datetime(final_df['month'])
            # final_df['month'] = final_df['month'].dt.strftime('%Y-%d-%m')  
            # print(type(final_df['month']),final_df['month'])
            month_list = ['2022-04-01','2022-05-01','2022-06-01','2022-07-01','2022-08-01','2022-09-01','2022-10-01','2022-11-01','2022-12-01','2023-01-01','2023-02-01','2023-03-01']
            final_df = final_df[final_df['month'].isin(month_list)]
            # Replace column value
            final_df.replace([np.inf, -np.inf], 0, inplace=True)
            outputfile = os.path.join(os.path.dirname(__file__))+'/mis/output_files/MIS Key Sheet.csv'
            final_df.to_csv(outputfile, index=False)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = exc_type+' '+fname+' '+exc_tb.tb_lineno+' '+str(e)
            logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.status = False

    def start_pipeline(self):
        # try:
        #     self.extract_ops_salary_sheet()
        #     self.get_finance_mis_source()
        #     self.get_query_kpis()
        #     self.logistics_last_mile_source()
        #     self.logistics_mid_mile_source()
        # except Exception as e:
        #     exc_type, exc_obj, exc_tb = sys.exc_info()
        #     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #     print(exc_type, fname, exc_tb.tb_lineno, str(e))
        #     logging.error(exc_type, fname, exc_tb.tb_lineno, str(e))
        # try:
        #     shutil.copy(os.path.join(os.path.dirname(__file__))+'/mis/input_files/Extracted Warehouse Area Sheet.csv', os.path.join(os.path.dirname(__file__))+'/mis/output_files/Extracted Warehouse Area Sheet.csv')
        # except:
            # pass
        self.merge_sheets()

# if __name__ == '__main__':
#     opsmis = OpsMIS()
#     opsmis.start_pipeline()
    # opsmis.merge_sheets()