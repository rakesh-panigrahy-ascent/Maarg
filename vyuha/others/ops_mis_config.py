import logging
import os
import pandas as pd
import numpy as np
import sys
from vyuha.others.ops_mis_consolidater import *

class MIS_Config(OpsMIS):
    def __init__(self):
        pass

    def calculate(self, all_kpis_df):
       # Calculate kpi headers# 
        all_kpis_df['Avg Order Value (Gross)'] = all_kpis_df['Gross Sales (W/o Gst)']*1000000/all_kpis_df['Billed orders (Total)']
        all_kpis_df['Avg Order Value (retail)'] = all_kpis_df['Net Revenue (after discuount W/o Gst)']*1000000/all_kpis_df['Billed orders ( Retail )']
        all_kpis_df['Sale Return Percentage'] = all_kpis_df['Sale Return Value from customer'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Expiry Return Percentage'] = all_kpis_df['Expiry Return Value from customer'] / all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Percentage of Non-moving item'] = all_kpis_df['Value of Non-moving item'] / all_kpis_df['Inventory value - PTS/EPR (15th of the month)']

        all_kpis_df['Total Operating Cost as a % of Net Revenue (before THEA reimbursement Deduction)'] = all_kpis_df['Total Operating Cost (before THEA reimbursement Deduction)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.A Salaries & Wages as a % of Net Revenue'] = all_kpis_df['1. Salaries & Wages']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Ops Salaries & Wages as a % of Net Revenue'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.b Delivery Salary % of Net Revenue'] = all_kpis_df['Total Salary--Delivery']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.i Inward % of Net Revenue'] = all_kpis_df['Total Salary--Inward']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.ii Store cost per order % of Net Revenue'] = all_kpis_df['Total Salary--Store']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.iii Checking % of Net Revenue'] = all_kpis_df['Total Salary--Checking']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.iv Dispatch % of Net Revenue'] = all_kpis_df['Total Salary--Dispatch']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.v Audit and refilling % of Net Revenue'] = all_kpis_df['Total Salary--Audit & Refilling']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.vi Expiry % of Net Revenue'] = all_kpis_df['Total Salary--Expiry']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.vii Sales return % of Net Revenue'] = all_kpis_df['Total Salary--Sales Return']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        # all_kpis_df['1.a.viii Overall Operations % of Net Revenue'] = all_kpis_df['Total Salary--Overall Operation']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.d Admin % of Net Revenue'] = all_kpis_df['Total Salary--Admin']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.e Finance & Accounts % of Net Revenue'] = all_kpis_df['Total Salary--Finance & Accounts']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.f Purchase % of Net Revenue'] = all_kpis_df['Total Salary--Purchase']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.g Sales % of Net Revenue'] = all_kpis_df['Total Salary--Sales']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.h IT % of Net Revenue'] = all_kpis_df['Total Salary--IT']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.i HR % of Net Revenue'] = all_kpis_df['Total Salary--HR']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.j Data Analyst % of Net Revenue'] = all_kpis_df['Total Salary--Data analyst']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['1.k Overall Non - Operations % of Net Revenue'] = all_kpis_df['Total Salary--Overall Non - Ops']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Total Last mile (Actual) as a % of Net Revenue'] = all_kpis_df['Total Last mile (Actual)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Mid mile (MIS) as a % of Net Revenue'] = all_kpis_df['Mid mile (MIS)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Last Mile (MIS) as a % of Net Revenue'] = all_kpis_df['Last Mile (MIS)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Total Last Mile (MIS + Inhouse Delivery) as a % of Net Revenue'] = (all_kpis_df['Last Mile (MIS)'] + all_kpis_df['Inhouse Bikers Cost'])/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Mid mile (Actual) as a % of Net Revenue'] = all_kpis_df['Mid mile (Actual)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['3.A. Packaging Cost as a % of Net Revenue'] = all_kpis_df['3. Packaging Cost']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['3.a Polybag cost Cost as a % of Net Revenue'] = all_kpis_df['3.a Polybag cost']/all_kpis_df['Billed orders (Total)']
        all_kpis_df['3.b other packing cost Cost as a % of Net Revenue'] = all_kpis_df['3.b other packing cost']/all_kpis_df['Billed orders (Total)']
        all_kpis_df['4.A. Electricity as a % of Net Revenue'] = all_kpis_df['4. Electricity']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['5.A. Telephone & Internet as a % of Net Revenue'] = all_kpis_df['5. Telephone & Internet']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['6.A. Commission expenses as a % of Net Revenue'] = all_kpis_df['6. Commission expenses']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['7.A. Warehouse Rent as a % of Net Revenue'] = all_kpis_df['7. Warehouse Rent']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['8.A. Cash Discounts to Retailers as a % of Net Revenue'] = all_kpis_df['8. Cash Discounts to Retailers']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['9.A. Other Operating Expenses as a % of Net Revenue'] = all_kpis_df['9. Other Operating Expenses']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['10.A. Other income as a % of Net Revenue'] = all_kpis_df['10. Other income']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['THEA Reimbursemnt as a % of Net Revenue'] = all_kpis_df['THEA reimbursement']/all_kpis_df['Net Revenue (after discuount W/o Gst)']
        all_kpis_df['Total Operating Cost as a % of Net Revenue (After THEA reimbursement Deduction)'] = all_kpis_df['Total Operating Cost (after THEA reimbursement Deduction)']/all_kpis_df['Net Revenue (after discuount W/o Gst)']

        all_kpis_df['Cost per man per month Total Operations'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Present Head Count Ops']*1000000
        all_kpis_df['Cost per man per month Inward'] = all_kpis_df['Total Salary--Inward'] / all_kpis_df['Present Head Count--Inward']*1000000
        all_kpis_df['Cost per man per month Store'] = all_kpis_df['Total Salary--Store'] / all_kpis_df['Present Head Count--Store']*1000000
        all_kpis_df['Cost per man per month Checking'] = all_kpis_df['Total Salary--Checking'] / all_kpis_df['Present Head Count--Checking']*1000000
        all_kpis_df['Cost per man per month Dispatch'] = all_kpis_df['Total Salary--Dispatch'] / all_kpis_df['Present Head Count--Dispatch']*1000000
        all_kpis_df['Cost per man per month Audit and refilling'] = all_kpis_df['Total Salary--Audit & Refilling'] / all_kpis_df['Present Head Count--Audit & Refilling']*1000000
        all_kpis_df['Cost per man per month Expiry'] = all_kpis_df['Total Salary--Expiry'] / all_kpis_df['Present Head Count--Expiry']*1000000
        all_kpis_df['Cost per man per month Sales return'] = all_kpis_df['Total Salary--Sales Return'] / all_kpis_df['Present Head Count--Sales Return']*1000000
        all_kpis_df['Cost per man per month Overall operations'] = all_kpis_df['Total Salary--Overall Operation'] / all_kpis_df['Present Head Count--Overall Operation']*1000000
        # all_kpis_df['Cost per man per month Total Operations'] = all_kpis_df['Cost per man per month Inward'] + all_kpis_df['Cost per man per month Store'] + all_kpis_df['Cost per man per month Checking'] + all_kpis_df['Cost per man per month Dispatch'] + all_kpis_df['Cost per man per month Audit and refilling'] + all_kpis_df['Cost per man per month Expiry'] + all_kpis_df['Cost per man per month Sales return'] + all_kpis_df['Cost per man per month Overall operations']
        all_kpis_df['Cost per man per month Delivery'] = all_kpis_df['Total Salary--Delivery'] / all_kpis_df['Present Head Count--Delivery']*1000000
        all_kpis_df['Cost per man per month Admin'] = all_kpis_df['Total Salary--Admin'] / all_kpis_df['Present Head Count--Admin']*1000000
        all_kpis_df['Cost per man per month Finance & Accounts'] = all_kpis_df['Total Salary--Finance & Accounts'] / all_kpis_df['Present Head Count--Finance & Accounts']*1000000
        all_kpis_df['Cost per man per month Purchase'] = all_kpis_df['Total Salary--Purchase'] / all_kpis_df['Present Head Count--Purchase']*1000000
        all_kpis_df['Cost per man per month Sales'] = all_kpis_df['Total Salary--Sales'] / all_kpis_df['Present Head Count--Sales']*1000000
        all_kpis_df['Cost per man per month IT'] = all_kpis_df['Total Salary--IT'] / all_kpis_df['Present Head Count--IT']*1000000
        all_kpis_df['Cost per man per month HR'] = all_kpis_df['Total Salary--HR'] / all_kpis_df['Present Head Count--HR']*1000000
        all_kpis_df['Cost per man per month Data Analyst'] = all_kpis_df['Total Salary--Data analyst'] / all_kpis_df['Present Head Count--Data analyst']*1000000
        all_kpis_df['Cost per man per month Overall Non-Operations'] = all_kpis_df['Total Salary--Overall Non - Ops'] / all_kpis_df['Present Head Count--Overall Non - Ops']*1000000
        all_kpis_df['Inward Productivity (Inward Strips Per Manday)'] = all_kpis_df['Inward Strips']/all_kpis_df['Worked Mandays--Inward']
        all_kpis_df['Store Productivity (Billed line Items per Manday)'] = all_kpis_df['Billed line items (Total)']/all_kpis_df['Worked Mandays--Store']
        all_kpis_df['Checking Productivity (Billed Strips Per Manday)'] = all_kpis_df['Billed Strips (Total)']/all_kpis_df['Worked Mandays--Checking']
        all_kpis_df['Dispatch Productivity (Billed Orders per Manday)'] = all_kpis_df['Billed orders (Total)']/all_kpis_df['Worked Mandays--Dispatch']
        all_kpis_df['Audit and refilling Productivity (Godown to Store Strips per Manday)'] = all_kpis_df['Godown to store Strips']/all_kpis_df['Worked Mandays--Audit & Refilling']
        all_kpis_df['Expiry Productivity (Expiry Return Strips Per Manday)'] = all_kpis_df['Expiry Return Strips']/all_kpis_df['Worked Mandays--Expiry']
        all_kpis_df['Sales return Productivity ( Sales Return Strips Per Manday)'] = all_kpis_df['Sales Return Strips']/all_kpis_df['Worked Mandays--Sales Return']
        all_kpis_df['Purchase Productivity ( Inward Value per Manday)'] = all_kpis_df['Inward value ( PTS/EPR)']/all_kpis_df['Worked Mandays--Purchase']
        all_kpis_df['Sales Productivity (Net Revenue Per Manday)'] = all_kpis_df['Net Revenue (after discuount W/o Gst)']/all_kpis_df['Worked Mandays--Sales']
        all_kpis_df['Inventory per sqft'] = all_kpis_df['Inventory value - PTS/EPR (15th of the month)']/all_kpis_df['Warehouse area']
        all_kpis_df['Net Revenue per sqft'] = all_kpis_df['Net Revenue (after discuount W/o Gst)'] * 1000000/all_kpis_df['Warehouse area']

        all_kpis_df['Cost Per Biker Per Month'] = (all_kpis_df['Biker'] + (all_kpis_df['Van'])*3) / all_kpis_df['Total Biker headcount']*1000000

        all_kpis_df['1. Salaries & Wages cost per order'] = all_kpis_df['1. Salaries & Wages']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.i Inward cost per order'] = all_kpis_df['Total Salary--Inward']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.ii Store cost per order'] = all_kpis_df['Total Salary--Store']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.iii Checking cost per order'] = all_kpis_df['Total Salary--Checking']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.iv Dispatch cost per order'] = all_kpis_df['Total Salary--Dispatch']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.v Audit and refilling cost per order'] = all_kpis_df['Total Salary--Audit & Refilling']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.vi Expiry cost per order'] = all_kpis_df['Total Salary--Expiry']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.vii Sales return cost per order'] = all_kpis_df['Total Salary--Sales Return']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.a.viii Overall Operations cost per order'] = all_kpis_df['Total Salary--Overall Operation']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.b Delivery Salary cost per order'] = all_kpis_df['Total Salary--Delivery']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.d Admin cost per order'] = all_kpis_df['Total Salary--Admin']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.e Finance & Accounts cost per order'] = all_kpis_df['Total Salary--Finance & Accounts']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.f Purchase cost per order'] = all_kpis_df['Total Salary--Purchase']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.g Sales cost per order'] = all_kpis_df['Total Salary--Sales']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.h IT cost per order'] = all_kpis_df['Total Salary--IT']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.i HR cost per order'] = all_kpis_df['Total Salary--HR']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.j Data Analyst cost per order'] = all_kpis_df['Total Salary--Data analyst']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['1.k Overall Non - Operations cost per order'] = all_kpis_df['Total Salary--Overall Non - Ops']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['3. Packaging Cost per order'] = all_kpis_df['3. Packaging Cost']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['3.a Polybag cost cost per order'] = all_kpis_df['3.a Polybag cost']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['3.b other packing cost cost per order'] = all_kpis_df['3.b other packing cost']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['4. Electricity per order'] = all_kpis_df['4. Electricity']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['5. Telephone & Internet per order'] = all_kpis_df['5. Telephone & Internet']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['6. Commission expenses per order'] = all_kpis_df['6. Commission expenses']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['7. Warehouse Rent per order'] = all_kpis_df['7. Warehouse Rent']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['8. Cash Discounts to Retailers per order'] = all_kpis_df['8. Cash Discounts to Retailers']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['9. Other Operating Expenses per order'] = all_kpis_df['9. Other Operating Expenses']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['10. Other income per order'] = all_kpis_df['10. Other income']/all_kpis_df['Billed orders (Total)']*1000000
        all_kpis_df['THEA reimbursement per order'] = all_kpis_df['THEA reimbursement']/all_kpis_df['Billed orders (Total)']*1000000

        all_kpis_df['Salary and wages cost per order'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Billed orders (Total)']*1000000
        
        all_kpis_df['Last Mile Cost/Order'] = all_kpis_df['Last Mile (MIS)']/all_kpis_df['Billed orders (Total)']
        all_kpis_df['Mid Mile Cost/Order'] = all_kpis_df['Mid mile (MIS)']/all_kpis_df['Billed orders (Total)']
        all_kpis_df['Water Charges/Order'] = all_kpis_df['Water Charges']/all_kpis_df['Billed orders (Total)']
        all_kpis_df['Last Mile Cost/Order'] = all_kpis_df['Last Mile Cost/Order'] * 1000000
        all_kpis_df['Mid Mile Cost/Order'] = all_kpis_df['Mid Mile Cost/Order'] * 1000000
        return all_kpis_df

    def calculate_kpis(self, all_kpis_df):
        try:
            # print(type(all_kpis_df))
            # print(all_kpis_df.columns.tolist())
            # Logistics kpis
            all_kpis_df['Last Mile Cost/Order'] = all_kpis_df['Last Mile (MIS)']/all_kpis_df['Billed orders (Total)']
            all_kpis_df['Mid Mile Cost/Order'] = all_kpis_df['Mid mile (MIS)']/all_kpis_df['Billed orders (Total)']
            all_kpis_df['Water Charges'] = all_kpis_df['Water Charges'] * 1000000
            all_kpis_df['Water Charges/Order'] = all_kpis_df['Water Charges']/all_kpis_df['Billed orders (Total)']
            all_kpis_df['Last Mile Cost/Order'] = all_kpis_df['Last Mile Cost/Order'] * 1000000
            all_kpis_df['Mid Mile Cost/Order'] = all_kpis_df['Mid Mile Cost/Order'] * 1000000
            
            all_kpis_df['Total Working Days'] = all_kpis_df['month'].apply(self.get_working_days)
            all_kpis_df['Daily Billed orders ( Retail )'] = all_kpis_df['Billed orders ( Retail )']/all_kpis_df['Total Working Days']
            all_kpis_df['Daily Billed orders (Inter company )'] = all_kpis_df['Billed orders (Inter company )']/all_kpis_df['Total Working Days']
            all_kpis_df['Daily Billed orders (Total)'] = all_kpis_df['Billed orders (Total)']/all_kpis_df['Total Working Days']

            

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
            # all_kpis_df['Cost per ManMonth - Salary & Wages'] = all_kpis_df['Cost per ManMonth - Salary & Wages'] * 1000000
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
            
            all_kpis_df['Cost per man per month Inward'] = all_kpis_df['Total Salary--Inward'] / all_kpis_df['Present Head Count--Inward']*1000000
            all_kpis_df['Cost per man per month Store'] = all_kpis_df['Total Salary--Store'] / all_kpis_df['Present Head Count--Store']*1000000
            all_kpis_df['Cost per man per month Checking'] = all_kpis_df['Total Salary--Checking'] / all_kpis_df['Present Head Count--Checking']*1000000
            all_kpis_df['Cost per man per month Dispatch'] = all_kpis_df['Total Salary--Dispatch'] / all_kpis_df['Present Head Count--Dispatch']*1000000
            all_kpis_df['Cost per man per month Audit and refilling'] = all_kpis_df['Total Salary--Audit & Refilling'] / all_kpis_df['Present Head Count--Audit & Refilling']*1000000
            all_kpis_df['Cost per man per month Expiry'] = all_kpis_df['Total Salary--Expiry'] / all_kpis_df['Present Head Count--Expiry']*1000000
            all_kpis_df['Cost per man per month Sales return'] = all_kpis_df['Total Salary--Sales Return'] / all_kpis_df['Present Head Count--Sales Return']*1000000
            all_kpis_df['Cost per man per month Overall operations'] = all_kpis_df['Total Salary--Overall Operation'] / all_kpis_df['Present Head Count--Overall Operation']*1000000
            # all_kpis_df['Cost per man per month Total Operations'] = all_kpis_df['Cost per man per month Inward'] + all_kpis_df['Cost per man per month Store'] + all_kpis_df['Cost per man per month Checking'] + all_kpis_df['Cost per man per month Dispatch'] + all_kpis_df['Cost per man per month Audit and refilling'] + all_kpis_df['Cost per man per month Expiry'] + all_kpis_df['Cost per man per month Sales return'] + all_kpis_df['Cost per man per month Overall operations']
            
            all_kpis_df['Cost per man per month Delivery'] = all_kpis_df['Total Salary--Delivery'] / all_kpis_df['Present Head Count--Delivery']*1000000
            all_kpis_df['Cost per man per month Admin'] = all_kpis_df['Total Salary--Admin'] / all_kpis_df['Present Head Count--Admin']*1000000
            all_kpis_df['Cost per man per month Finance & Accounts'] = all_kpis_df['Total Salary--Finance & Accounts'] / all_kpis_df['Present Head Count--Finance & Accounts']*1000000
            all_kpis_df['Cost per man per month Purchase'] = all_kpis_df['Total Salary--Purchase'] / all_kpis_df['Present Head Count--Purchase']*1000000
            all_kpis_df['Cost per man per month Sales'] = all_kpis_df['Total Salary--Sales'] / all_kpis_df['Present Head Count--Sales']*1000000
            all_kpis_df['Cost per man per month IT'] = all_kpis_df['Total Salary--IT'] / all_kpis_df['Present Head Count--IT']*1000000
            all_kpis_df['Cost per man per month HR'] = all_kpis_df['Total Salary--HR'] / all_kpis_df['Present Head Count--HR']*1000000
            all_kpis_df['Cost per man per month Data Analyst'] = all_kpis_df['Total Salary--Data analyst'] / all_kpis_df['Present Head Count--Data analyst']*1000000
            all_kpis_df['Cost per man per month Overall Non-Operations'] = all_kpis_df['Total Salary--Overall Non - Ops'] / all_kpis_df['Present Head Count--Overall Non - Ops']*1000000
            all_kpis_df['Total Biker headcount'] = (all_kpis_df['Present Head Count--Delivery'] + all_kpis_df['3rd Biker Headcount'])+(3 * all_kpis_df['3rd Van Headcount'])
            all_kpis_df['Cost Per Biker Per Month'] = (all_kpis_df['Biker'] + (all_kpis_df['Van'])*3) / all_kpis_df['Total Biker headcount']*1000000
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
            all_kpis_df['Cost per man per month Total Operations'] = all_kpis_df['Ops Salaries & Wages']/all_kpis_df['Present Head Count Ops']*1000000
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
                    'Ops Salaries & Wages as a % of Net Revenue','Present Head Count Ops','Cost per man per month Total Operations', 'Daily Billed orders ( Retail )', 'Daily Billed orders (Inter company )', 'Daily Billed orders (Total)', 'Water Charges/Order', 'Last Mile Cost/Order', 'Mid Mile Cost/Order', 'Water Charges']]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, str(e))
            self.error = str(exc_type)+' '+str(fname)+' '+str(exc_tb.tb_lineno)+' '+str(e)
            self.status = False
            logging.info(str(exc_type)+' '+str(fname)+' '+str(exc_tb.tb_lineno)+' '+str(e))
        return all_kpis_df