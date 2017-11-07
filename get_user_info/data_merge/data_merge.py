#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo.mongo_basicinfo import mongo_basicinfo
from get_user_info.data_from_mongo.mongo_pcrinfo import mongo_pcrinfo
from get_user_info.data_from_mongo.mongo_additionalinfo import mongo_additionalinfo
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
from datetime import datetime
from get_user_info.connect_database.get_mysql_collection import get_cif_M2,\
    get_cdss_txntime
import logging
import time


#cdss_df=get_cdss_txntime()

def get_phone_city(phone):

    url="http://www.2dianying.net/search/"

    api_url="http://open.onebox.so.com/dataApi?query=%s&url=mobilecheck&num=1&type=mobilecheck&src=onebox"

    #phone_list=['15280013634','13530891520','13588645128']
    #city_list=[]
    respone = urllib.request.Request(url + str(phone))
    html = urllib.request.urlopen(respone)
    web = BeautifulSoup(html, 'lxml')
    ls=web.find_all('td')

    for i in range(len(ls)):
        item=ls[i]
        if len(item)==2:
            phone_city=item.a.string

            return phone_city



def data_merge(item):

    logger = logging.getLogger(__name__)
    starttime = time.time()
    logger.info('to get cdss_txntime begin')

    basic_info = mongo_basicinfo()
    var_applyid=basic_info.get_applyid(item)

    #排除applyid开头为T的数据
    if list(var_applyid)[0]!='T':
        var_partyid=basic_info.get_partyid(item)
        var_phone=basic_info.get_phone(item)

        #basicinfo
        var_age=basic_info.get_age(item)
        var_gender=basic_info.get_gender(item)
        var_marr=basic_info.get_marr(item)
        var_city=basic_info.get_city(item)
        var_phone_city=get_phone_city(var_phone)

        pcr_info=mongo_pcrinfo()

        #pcrinfo
        var_creditcard_num=pcr_info.get_credit_num(item)
        var_loan_num=pcr_info.get_credit_num(item)
        var_higest_quota=pcr_info.get_creditcard_higest_quota(item)
        var_overdue_num=pcr_info.get_history_overduenum(item)
        var_card_userate=pcr_info.get_creditcard_userate(item)
        var_inquiry_num=pcr_info.get_credit_inquiry(item)

        additional_info = mongo_additionalinfo()

        #additionalinfo
        var_zm_score=additional_info.get_zmscore(item)
        var_td_score=additional_info.get_tdscore(item)
        var_contact=additional_info.get_contact(item)

        '''
        logger.info('get mysql data begin')

        
        if var_partyid in cdss_df['partyid'].unique():
            var_first_txntime=cdss_df[cdss_df['partyid']==var_partyid]['firsttime'].values[0]
            var_last_txntime=cdss_df[cdss_df['partyid']==var_partyid]['lasttime'].values[0]
        else:
            var_first_txntime='None'
            var_last_txntime='None'
            ,'first_txntime':var_first_txntime,'last_txntime':var_last_txntime
        '''

        result_dict={'applyid':var_applyid,'age':var_age,'gender':var_gender,'marr':var_marr,'city':var_city,
                     'creditcard_num':var_creditcard_num,'loan_num':var_loan_num,'higest_quota':var_higest_quota,
                     'overdue_num':var_overdue_num,'creditcard_userate':var_card_userate,'inquiry_num':var_inquiry_num,
                     'zm_score':var_zm_score,'partyid':var_partyid,'phone_city':var_phone_city,'contacts':var_contact,
                     'td_score':var_td_score}

        return result_dict


    else:
        return 'None'

    endtime = time.time()
    logger.info('end of  data  merge fromTime=[%s], toTime=[%s].' % (starttime, endtime))
