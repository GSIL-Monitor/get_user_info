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

    startime=time.time()
    logger.info('to get m2_df begin')

    #m2_df=get_cif_M2()

    middle_time_1=time.time()

    logger.info('get mongodb data, fromTime=[%s], toTime=[%s].' % (startime, middle_time_1))

    mongo_lrds=get_lrds_maindoc()


    key_list=['partyid','applyid','age','gender','marr','city','creditcard_num','loan_num','higest_quota',
              'overdue_num','creditcard_userate','inquiry_num','zm_score','phone_city','contacts',
              'td_score']

    result_list = []
    #for item in mongo_lrds.find({'crtTime':{'$gte':datetime(2017,1,1)}}):
    #for item in collection.find({'loanApplyInfo.data.partyId':{'$in':partyid_list}}):
    #for item in mongo_lrds.find().sort('crtTime',-1).limit(10):
    logger.info('star loop')
    x=0
    for item in mongo_lrds.find(no_cursor_timeout=True):
        x=x+1
        print(x,time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()))
        merge_dict=data_merge(item)
        logger.info('star if')
        if merge_dict=='None':
            continue
        else:
            turn_list=[]
            logger.info('begin second loop')
            for keys in key_list:
                turn_list.append(merge_dict[keys])
            result_list.append(turn_list)
            logger.info('end if')

    middle_time_2=time.time()
    logger.info('get mongodb data, fromTime=[%s], toTime=[%s].' % (middle_time_1, middle_time_2))

    all_info_df=pd.DataFrame(result_list,columns=key_list)

    #选取最大applyID
    applyid_df=all_info_df.groupby(all_info_df['partyid']).agg({'applyid':'max'}).reset_index()
    applyid_serise=applyid_df['applyid']

    #按最大applyid进行过滤
    end_all_info_df=all_info_df[all_info_df['applyid'].isin(applyid_serise)]

    #与逾期数据融合
    end_all_info_df=pd.merge(m2_df,end_all_info_df,on='partyid',how='left')
    endtime=time.time()

    logger.info('data handle, fromTime=[%s], toTime=[%s].' % (middle_time_2, endtime))

    return end_all_info_df



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

