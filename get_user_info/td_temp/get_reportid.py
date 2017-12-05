#!/usr/bin/python
# encoding=utf-8


from get_user_info.data_from_mongo.dict_parse import dict_parse
import os
import pandas as pd
from get_user_info.connect_database.get_mongo_collection import get_lrds_maindoc
from get_user_info.config import init_app
from get_user_info.data_merge.send_email import EmailSend

path=os.path.dirname(__file__)
path=path+'/partyid1.5.xlsx'
party_df=pd.read_excel(path)
applyid=list(party_df['APPLYID'].astype(str))

def get_phone(item):
    key_list=['loanApplyInfo','data','mobile']
    mobile=dict_parse(item,key_list,3)

    return mobile


def get_reportid(item):
    key_list=['tdReport','data','report_id']
    reportid=dict_parse(item,key_list,3)

    return reportid



def get_all_value(id_list):
    init_app()

    table=get_lrds_maindoc()

    all_list=[]
    for item in table.find({'applyId': {'$in': id_list}}):
        applyid=item['applyId']
        phone=get_phone(item)
        reportid=get_reportid(item)

        print([applyid,phone,reportid])
        all_list.append([applyid,phone,reportid])

    result_df=pd.DataFrame(all_list,columns=['APPLYID','PHONE','REPORTID'])


    return result_df





def email_task():

    res_df = get_all_value(applyid)
    end_df=pd.merge(party_df,res_df,on='APPLYID',how='outer')

    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/reportid.xlsx',engine='xlsxwriter')
    end_df.to_excel(excel_writer,index=False)
    excel_writer.save()

    subject = 'relative_phone'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'relative_phone'
    attachment_file = "/home/andpay/data/excel/reportid.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])



