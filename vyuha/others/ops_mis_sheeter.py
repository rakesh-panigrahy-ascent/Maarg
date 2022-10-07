import os
import sys
from turtle import bgcolor

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

driver,sheeter = sq.apiconnect()

def main():
    mis_pools_data = sq.sheetsToDf(sheeter,spreadsheet_id='15D-4rRRzzMxcuUsOXk_Rp8Gs4_smLXk5PrpTPlwvv6Y',sh_name='Unit - Pool')
    mis_kpis = sq.sheetsToDf(sheeter,spreadsheet_id='15D-4rRRzzMxcuUsOXk_Rp8Gs4_smLXk5PrpTPlwvv6Y',sh_name='temp')
    
    pools = mis_pools_data.iloc[:,1:].drop_duplicates()
    kpis = mis_kpis.iloc[:,0:]

    mis_file = os.path.join(os.path.dirname(__file__), 'mis/output_files/Final MIS.xlsx')
    workbook = xlsxwriter.Workbook(mis_file)
    # Add a bold format to use to highlight cells.
    bold = workbook.add_format({'bold': True})
    italic = workbook.add_format({'italic': True})

    for pool in pools.values:
        #Zone wise ditributors
        zone_wise_distributors = mis_pools_data[mis_pools_data['Pool']==pool[0]]['Distributor Name']

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
                cell_format = workbook.add_format(cell_format)
            else:
                cell_format = {}

            worksheet.write(row, col, kpi[0], cell_format)

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
                    formulae = '{}({})'.format(formula_type, refered_formula_cell)
                    worksheet.write_formula(row=kpi_row, col=formula_target_col, formula=formulae, cell_format=bold)
                    formula_target_col += 1
                
                row += 1
            current_row = row
            row += 1
        # KPI Loop End

    workbook.close()
    
    try:
        subject = 'Operation MIS Completed'
        to = ['rakesh.panigrahy@ahwspl.com', 'ritesh.thakre@ahwspl.com'] 
        cc = ['abhishek.mehta@ahwspl.com']
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