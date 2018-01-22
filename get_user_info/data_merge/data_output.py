#!/usr/bin/python
# encoding=utf-8

from get_user_info.connect_database.get_mysql_collection import get_cif_M2
from get_user_info.data_merge.data_merge import data_merge
from get_user_info.connect_database.get_mongo_collection import get_lrds_maindoc,get_cif_partyadditioninfo
import json
from datetime import datetime
import pandas as pd
from get_user_info.data_merge.send_email import EmailSend
import time
import logging
from get_user_info.config import init_app
from get_user_info.data_merge.data_center import data_center

def out_put_run():
    init_app()
    logger = logging.getLogger(__name__)

    startime=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    #logger.info('to get m2_df begin')
    #m2_df=get_cif_M2()
    mongo_lrds=get_lrds_maindoc()

    key_name=data_center({},'name')

    count=0
    data_soure=[]
    result_list = []
    #for item in mongo_lrds.find({'crtTime':{'$gte':datetime(2017,1,1)}}):
    #for item in collection.find({'loanApplyInfo.data.partyId':{'$in':partyid_list}}):
    #for item in mongo_lrds.find().sort('crtTime',-1).limit(10):
    for item in mongo_lrds.find(no_cursor_timeout=True).batch_size(500):
        data_soure.append(item)
        count=count+1
        if count==500:
            #print(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()))
            for item in data_soure:
                key_value=data_center(item,'value')
                if key_value is None:
                    continue
                else:
                    print(key_value)
                    result_list.append(key_value)
            data_soure=[]
            count=0

    all_info_df=pd.DataFrame(result_list,columns=key_name)

    #选取最大applyID
    #applyid_df=all_info_df.groupby(all_info_df['partyid']).agg({'applyid':'max'}).reset_index()
    #applyid_serise=applyid_df['applyid']

    #按最大applyid进行过滤
    #end_all_info_df=all_info_df[all_info_df['applyid'].isin(applyid_serise)]

    #与逾期数据融合
    #end_all_info_df=pd.merge(m2_df,end_all_info_df,on='partyid',how='left')

    endtime=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    logger.info('data handle, fromTime=[%s], toTime=[%s].' % (startime, endtime))

    return all_info_df



def email_task():

    '''
    score_df=out_put_run()
    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/get_user_info_2.xlsx',engine='xlsxwriter')
    score_df.to_excel(excel_writer,index=False)
    excel_writer.save()
    '''

    subject = 'get_user_info'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'get_user_info'
    attachment_file = "/home/andpay/data/excel/get_user_info_2.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])



'''
table=out_put_run()

excel_writer=pd.ExcelWriter('dataw.xlsx')
table.to_excel(excel_writer,'info',index=False)
excel_writer.save()
'''