import os
import sys

from vyuha.mailer.common_mailer import send_mail
sys.path.insert(0, os.getcwd())
from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine, false, text
import xlsxwriter
import vyuha.sheetioQuicks as sq
import json
from xlsxwriter.utility import xl_rowcol_to_cell
from vyuha.mailer.mailer import *
import numpy as np
from vyuha.others.ops_mis_config import *

driver,sheeter = sq.apiconnect()

def main():
    mis_pools_data = sq.sheetsToDf(sheeter,spreadsheet_id='15D-4rRRzzMxcuUsOXk_Rp8Gs4_smLXk5PrpTPlwvv6Y',sh_name='Unit - Pool')
    mis_pools_data = mis_pools_data[mis_pools_data['active']=='1']
    mis_kpis = sq.sheetsToDf(sheeter,spreadsheet_id='15D-4rRRzzMxcuUsOXk_Rp8Gs4_smLXk5PrpTPlwvv6Y',sh_name='temp')
    
    #### TO ADD Header KPI values #####
    kpis_df = pd.read_csv(os.path.join(os.path.dirname(__file__))+'/mis/output_files/Merged Calculated Sheet.csv')
    unit_name_df = pd.DataFrame(mis_pools_data)
    group_list = unit_name_df.iloc[:,:2].groupby('Pool')['Distributor Name'].apply(list)
    print(group_list)

    ############# END KPI Values ###############

    pools = mis_pools_data.iloc[:,1:].drop_duplicates()
    kpis = mis_kpis.iloc[:,0:]

    mis_file = os.path.join(os.path.dirname(__file__), 'mis/output_files/Final MIS.xlsx')
    workbook = xlsxwriter.Workbook(mis_file)
    # Add a bold format to use to highlight cells.
    bold = workbook.add_format({'bold': True})
    italic = workbook.add_format({'italic': True})
    print('pools', pools.values)
    print('Consolidateed')
    print(kpis_df.head())
    for pool in pools.values:
        #Zone wise ditributors
        zone_wise_distributors = mis_pools_data[mis_pools_data['Pool']==pool[0]]['Distributor Name']

        #### Adding df for KPI heders #####
        unit_list = group_list[pool[0]]
        all_kpis_df = kpis_df[kpis_df['unit_name'].isin(unit_list)]
        all_kpis_df = all_kpis_df.groupby('month').sum()
        mis_config = MIS_Config()
        all_kpis_df = mis_config.calculate(all_kpis_df=all_kpis_df)
        all_kpis_df.to_csv('all_kpis_df_{}.csv'.format(pool[0]), index=False)
        print('all_kpis_df')
        print(all_kpis_df)
        print('Exported!')
        

        ####### End KPI headers #########

        # Headers set start
        worksheet = workbook.add_worksheet(pool[0])

        headers = (
            ['Ascent', 'Key']
            )
        
        row = 0
        col = 0

        worksheet.write(row, col, 'Ascent', bold)
        col += 1
        worksheet.write(row, col, 'Key', bold)

        today = datetime.today()
        current_month = datetime.today().month
        current_year = datetime.today().year

        if current_month in (1,2,3):
            start_year = current_year - 1
        else:
            start_year = current_year

        current_date = start_date = datetime(start_year, 4, 1)

        while current_date <= today:
            print(current_date.date())
            col += 1
            print(col)
            worksheet.write(row, col, str(current_date.date()), bold)
            current_date = current_date + relativedelta(months=+1)
            max_date_col = col
            
        # Headers set end

        # KPI Loop Start
        row = 1
        col = 0
        for kpi in kpis.values:
            # Write KPI
            if kpi[6] != None:
                cell_format = json.loads(kpi[6])
                print(cell_format)
                cell_format = workbook.add_format(cell_format)
            else:
                cell_format = {}

            worksheet.write(row, col, kpi[0], cell_format)


            #### Adding df for KPI heders ####
            not_req_kpi = ['Unit Wise','Gross Sales (W/o Gst)', 'Intercompany Sales (W/o Gst)', 'Net Revenue (after discuount W/o Gst)', 'Billed orders (Total)',
                            'Billed orders (Inter company )', 'Billed orders ( Retail )', 'Billed line items (Total)', 'Billed line items (Inter company)',
                            'Billed line items (Retail)', 'Billed Strips (Total)', 'Billed Strips (Inter company)', 'Billed Strips (Retail)',
                            'Inward Strips', 'Godown to store Strips', 'Expiry Return Strips', 'Sales Return Strips',
                            'Inward value ( PTS/EPR)', 'Inventory value - PTS/EPR (15th of the month)',
                            'Sale Return Value from customer', 'Expiry Return Value from customer', 'Intransit breakage (Retail)',
                            'Expiry from godown', 'Gowdown breakage', 'Expiry to supplier', 'Expiry return strip to supplier', 'Value of Non-moving item','Warehouse area',
                            
                            'Total Operating Cost (before THEA reimbursement Deduction)', 'Department Wise Unit wise ( incl OT)',
                            '1. Salaries & Wages','Department Wise Unit wise ( incl OT)', 'Ops Salaries & Wages',
                            #  'Total Salary--Inward', 'Total Salary--Store', 'Total Salary--Checking', 'Total Salary--Dispatch',
                            #  'Total Salary--Audit & Refilling', 'Total Salary--Expiry', 'Total Salary--Sales Return', 'Total Salary--Overall Operation',
                            'Total Salary--Delivery', 'Total Salary--Admin', 'Total Salary--Finance & Accounts', 'Total Salary--Purchase', 'Total Salary--Sales',
                            'Total Salary--IT', 'Total Salary--HR', 'Total Salary--Data analyst', 'Total Salary--Overall Non - Ops','Total Last Mile (MIS + Inhouse Delivery)',
                            'Last Mile (MIS)','1.a.ii Vehicle running & mainteinance cost (Actual)','Payment to outside Agency','Dialhealth Delivery Cost', 'Mid mile (MIS)','Inhouse Bikers Cost',
                            'Mid mile (Actual)','Total Last mile (Actual)', 'Biker','Van','Supervisor','Provision Last mile Payment to ourside agency',
                            'Provision Mid mile', '3. Packaging Cost', '3.a Polybag cost', 'c ', '4. Electricity',
                            '5. Telephone & Internet', '6. Commission expenses', '7. Warehouse Rent', '8. Cash Discounts to Retailers', '9. Other Operating Expenses',
                            'Printing & stationery','9.a. Printing Cost', '9.b. Paper Cost', '9.c. other stationary Cost', 'Others', '10. Other income', 'THEA reimbursement',
                            'Total Operating Cost (after THEA reimbursement Deduction)', 'Inhouse Bikers Cost',
                            
                            'Present Headcount','Dept Wise','Present Head Count Ops',
                            #  'Present Head Count--Inward', 'Present Head Count--Store', 'Present Head Count--Checking', 'Present Head Count--Dispatch',
                            #  'Present Head Count--Audit & Refilling', 'Present Head Count--Expiry', 'Present Head Count--Sales Return',
                            #  'Present Head Count--Overall Operation',
                            'Present Head Count--Delivery', 'Present Head Count--Admin', 'Present Head Count--Finance & Accounts',
                            'Present Head Count--Purchase', 'Present Head Count--Sales', 'Present Head Count--IT', 'Present Head Count--HR',
                            'Present Head Count--Data analyst', 'Present Head Count--Overall Non - Ops',
                            'Delivery Present Headcount (Biker)','Present Head Count--3rd party Delivery','Total Biker headcount','3rd Supervisor Headcount',
                            '3rd Biker Headcount','3rd Van Headcount',
                            
                            'Cost per ManMonth - Salary & Wages',
                            'Productivity',
                            'Salary and wages cost per order','Department Wise cost per order','Ops Salary cost per order',
                            
                            'Worked Mandays--Inward', 'Worked Mandays--Store',
                            'Worked Mandays--Checking', 'Worked Mandays--Dispatch', 'Worked Mandays--Audit & Refilling', 'Worked Mandays--Expiry',
                            'Worked Mandays--Sales Return', 'Worked Mandays--Overall Operation', 'Worked Mandays--Delivery', 'Worked Mandays--Admin',
                            'Worked Mandays--Finance & Accounts', 'Worked Mandays--Purchase', 'Worked Mandays--Sales', 'Worked Mandays--IT',
                            'Worked Mandays--HR', 'Worked Mandays--Data analyst', 'Worked Mandays--Overall Non - Ops',
                            '3rd Supervisor Headcount', '3rd Biker Headcount', '3rd Van Headcount',
                            'Total Biker headcount', 'Payment to outside Agency', 'Dialhealth Delivery Cost', 
                            'Ops Salaries & Wages', 'Present Head Count Ops', 'Daily Billed Orders', 'Logistic Cost/Order']

            if kpi[0] not in not_req_kpi:
                current_date_1 = start_date
                col_1 = col+2
                while current_date_1 <= today:
                    all_kpis_month_df = all_kpis_df.loc[str(current_date_1.date())]
                    all_kpis_month_df = all_kpis_month_df.replace(np.inf, 0)
                    all_kpis_month_df = all_kpis_month_df.replace(np.nan,0)
                    # all_kpis_month_df.to_csv(os.path.join(os.path.dirname(__file__))+'/mis/output_files/test.csv')
                    print(current_date_1.date())
                    worksheet.write(row, col_1, all_kpis_month_df.loc[kpi[0]])
                    current_date_1 = current_date_1 + relativedelta(months=+1)
                    col_1 += 1
            ####### End KPI headers #########

            #Grouping
            if kpi[1] != 'main' and kpi[3] != None:
                worksheet.set_row(row, None, None, {'level': int(kpi[3])})

            if kpi[2] == 'Yes':
                kpi_row = row
                key_start_row = row + 1
                for dist in zone_wise_distributors:
                    row += 1

                    # Write Distributor
                    worksheet.write(row, col, dist)

                    # Write Key
                    worksheet.write(row, col+1, kpi[4]+'_'+dist)


                    # Fill Data
                    current_date = start_date
                    target_value_column = 2
                    # today = pd.to_datetime('2022-04-24')
                    while current_date <= today:
                        dfill = DataFill()
                        # print(current_date.date())
                        result = dfill.get_data(current_date.date(), kpi[4]+'_'+dist)
                        # print("Result",result,row,target_value_column)
                        worksheet.write(row, target_value_column, result)
                        current_date = current_date + relativedelta(months=+1)
                        target_value_column += 1


                    if kpi[3] != None:
                        worksheet.set_row(row, None, None, {'level': int(kpi[3])+1})
                key_end_row = row

                formula_target_col = 2
                while formula_target_col <= max_date_col:
                    refered_formula_cell = xl_rowcol_to_cell(key_start_row, formula_target_col)+':'+xl_rowcol_to_cell(key_end_row, formula_target_col)
                    formula_type = kpi[5]
                    # if ('percentage' in kpi[1].lower()) or ('%' in kpi[1]):
                    #     formulae = '{}({}({}({})*100,2),"%")'.format('CONCATENATE','ROUND',formula_type, refered_formula_cell)
                    # else:
                    #     formulae = '{}({}({}),2)'.format('ROUND',formula_type, refered_formula_cell)
                    if kpi[5] != 'blank':
                        formulae = '{}({})'.format(formula_type, refered_formula_cell)
                    else:
                        formulae = ""

                    worksheet.write_formula(row=kpi_row, col=formula_target_col, formula=formulae, cell_format=bold)
                    formula_target_col += 1
            
                row += 1
            current_row = row
            row += 1
        # KPI Loop End

    workbook.close()
    
    try:
        subject = 'Operation MIS Completed'
        to = ['rakesh.panigrahy@ahwspl.com'] 
        cc = ['mohan.yadav@ahwspl.com']
        text = mail_body()
        attachment_path = mis_file
        send_mail(subject, to, cc, text, attachment_path)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, str(e))
    

class DataFill:
    def __init__(self):
        self.fill_data_status = True
        self.message = ''
        try:
            self.consolidated_sheet = pd.read_csv(os.path.join(os.path.dirname(__file__), 'mis/output_files/MIS Key Sheet.csv'))
            self.consolidated_sheet['month'] = pd.to_datetime(self.consolidated_sheet['month'])
            self.consolidated_sheet['month'] = self.consolidated_sheet['month'].dt.strftime('%Y-%m-%d')
        except Exception as e:
            self.message = 'File Not Found !'
            self.fill_data_status = False

    def get_data(self, target_date, target_kpi):
        try:
            if self.fill_data_status == True:
                # print(self.consolidated_sheet['month'],target_date,'2022-04-01'==target_date)
                # target_date = str(target_date)
                # # self.consolidated_sheet['month'] = str(self.consolidated_sheet['month'])
                # print(target_date, type(target_date))
                # print(type(self.consolidated_sheet['month']),self.consolidated_sheet['month'])
                print(target_date,target_kpi)
                result = self.consolidated_sheet[(self.consolidated_sheet['key']==target_kpi) & (self.consolidated_sheet['month']==str(target_date))].iloc[:,3].values[0]
                # if ('percentage' in target_kpi.lower()) or ('%' in target_kpi):
                #     result = round((result*100),2)
                # else:
                #     result = round(result, 2)
                print(result)
            else:
                result = self.message
        except Exception as e:
            result = 'KPI Not Found !'
        return result


def mail_body():
    msg = """<p><b>Operation MIS Completed</b><br>
                Kindly find the attachment
            </p>  
        """
    return msg