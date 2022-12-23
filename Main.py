#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 00:10:41 2022

@author: Hemantdayma
"""

import pandas as pd
import mysql.connector as connect
import os,sys
import xlsxwriter
import warnings
warnings.filterwarnings('ignore')


def ivr_data(hosts,writer,start_date,end_date):
    ivr_df = pd.DataFrame()
    sms_df = pd.DataFrame()
    for host in hosts:
        if 'v3' in host:
            file_name = 'HDFC_Predue_Voice_logs'
        else:
            file_name = 'HDFC_Postdue_Voice_logs'
        try:    
            conn = connect.connect(host=host,
                                database='admin_engage',
                                user='sp_service_user',
                                password='N(%GHw_Gg@E@T2C^',
                                port =3306)
            print("Succeesfully connect to",host)
            cur = conn.cursor()
            Query = '''SELECT sl.batch_no,sl.sp_product,sl.sp_account_number,sl.mobile,svl.sp_call_answered as `DELIVERY_STATUS`,
                        cast(svl.sp_call_pickup_time as Date) as `DELIVERY_DATE`,LEFT(cast(svl.sp_call_pickup_time as Time),5) as `DELIVERY_TIME`,
                        svl.sp_call_action,svm.name as `content_name`,DAYNAME(CAST(svl.sp_call_pickup_time as Date)) as `DAY`
                        from sp_voice_log svl
                        left join sp_leads sl
                        on svl.sp_lead_id = sl.id 
                        left join sp_voice_messages svm 
                        on svm.id = svl.content_id 
                        WHERE CAST(svl.sp_call_pickup_time AS Date) BETWEEN '{}' and '{}' 
                        and sl.customerid != 99
                        and sl.batch_no != 99
                        and CAST(svl.sp_call_pickup_time AS Time) BETWEEN '09:30:00' and '18:29:58'
                        '''.format(start_date, end_date)
            cur.execute(Query)
            colnames = [desc[0] for desc in cur.description]
            data = pd.DataFrame(cur.fetchall())
            if len(data) > 0:
                data.columns = colnames
                ivr_df = ivr_df.append(data)
            cur.close()
            conn.close()
            data_sorting(ivr_df,sms_df,file_name,writer)
            print('Extract Data Succeesfully for voice..')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, e)
    return ivr_df

def sms_data(hosts,writer,start_date,end_date):
    ivr_df = pd.DataFrame()
    sms_df = pd.DataFrame()
    for host in hosts:
        if 'v3' in host:
            file_name = 'HDFC_Predue_SMS_logs'
        else:
            file_name = 'HDFC_Postdue_SMS_logs'
        try:    
            conn = connect.connect(host=host,
                                database='admin_engage',
                                user='sp_service_user',
                                password='N(%GHw_Gg@E@T2C^',
                                port =3306)
            print("Succeesfully connect to",host)
            cur = conn.cursor()
            Query = '''SELECT sl.batch_no,sl.sp_product,sl.sp_account_number,sl.mobile,sms.sp_message_status as DELIVERY_STATUS,
                        CAST(sms.delivered_time as Date) as `DELIVERY_DATE`,LEFT(CAST(sms.delivered_time as Time),5) as `DELIVERY_Time`,svm.name as `content_name`,	
                        DAYNAME(CAST(sms.delivered_time as Date)) as `DAY`
                        from sp_sms_log sms
                        left join sp_leads sl
                        on sms.sp_lead_id = sl.id 
                        left join sp_sms_messages svm 
                        on svm.id = sms.content_id
                        WHERE CAST(sms.delivered_time AS Date) BETWEEN '{}' and '{}'
                        and sl.customerid != 99
                        and sl.batch_no != 99
                        and CAST(sms.delivered_time AS Time) BETWEEN '09:30:00' and '18:29:58'
                        '''.format(start_date, end_date)
            cur.execute(Query)
            colnames = [desc[0] for desc in cur.description]
            data = pd.DataFrame(cur.fetchall())
            if len(data) > 0:
                data.columns = colnames
                sms_df = sms_df.append(data)
            cur.close()
            conn.close()
            data_sorting(ivr_df,sms_df,file_name,writer)
            print('Extract Data Succeesfully for sms..')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno, e)
    return sms_df


def data_sorting(ivr_df,sms_df,file_name,writer):
    try:
        if ivr_df.shape[0]>0:
            ivr_df.to_excel(writer,sheet_name=file_name,index=False)
        if sms_df.shape[0]>0:
            sms_df.to_excel(writer,sheet_name=file_name,index=False)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, e)
  

if __name__ == '__main__':
    try:
        date_list = [['2022-12-01','2022-12-05'],['2022-12-05','2022-12-10'],
                    ['2022-12-11','2022-12-15'],['2022-12-15','2022-12-20'],['2022-12-21','2022-12-23']]
        
        # date_list = [['2022-12-01','2022-12-05'],['2022-12-06','2022-12-10']]
        
        for date in date_list:
            print(date[0],date[1])
            path = os.getcwd()
            writer = pd.ExcelWriter(path+'/HDFC_logs_Audit_{}_{}.xlsx'.format(date[0],date[1]), 
                                    engine='xlsxwriter')
            # Host Name For data Pulling..
            hosts = ['engage2605.cluster-ro-cs4ck8i0kklf.ap-south-1.rds.amazonaws.com','enagge-2605v3.cluster-ro-cs4ck8i0kklf.ap-south-1.rds.amazonaws.com']
            
            # IVR_DATA Pulling....
            
            ivr_data(hosts,writer,date[0],date[1])
            
            # SMS_DATA Pulling....
            
            sms_data(hosts,writer,date[0],date[1])
            
            # Save Excel File....
            
            writer.save()
        print('All Process Done...')
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno, e)
