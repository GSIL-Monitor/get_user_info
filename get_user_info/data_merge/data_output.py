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

def out_put_run():

    init_app()
    logger = logging.getLogger(__name__)

    startime=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    #logger.info('to get m2_df begin')
    #m2_df=get_cif_M2()
    mongo_lrds=get_lrds_maindoc()
    print(mongo_lrds)

    key_list=['partyid','applyid','age','gender','marr','city','creditcard_num','loan_num','higest_quota',
              'overdue_num','creditcard_userate','inquiry_num','contacts','td_score','zm_score','zm_atfscore',
              'zm_watchlist','zm_risklist']

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
                merge_dict=data_merge(item)
                print(merge_dict)
                if merge_dict=='None':
                    continue
                else:
                    turn_list=[]
                    for keys in key_list:
                        turn_list.append(merge_dict[keys])
                    result_list.append(turn_list)
            data_soure=[]
            count=0

    all_info_df=pd.DataFrame(result_list,columns=key_list)

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

    score_df=out_put_run()
    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/get_user_info.xlsx',engine='xlsxwriter')
    score_df.to_excel(excel_writer,index=False)
    excel_writer.save()

    subject = 'get_user_info'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'get_user_info'
    attachment_file = "/home/andpay/data/excel/get_user_info.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])


out_put_run()