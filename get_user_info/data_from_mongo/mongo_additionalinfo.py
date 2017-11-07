#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo import dict_parse
from get_user_info.connect_database.get_mongo_collection import get_lrds_maindoc
import pandas as pd
import datetime as dt



class mongo_additionalinfo():

    def get_zmscore(self,data):
        key_list = ['zmxyReport', 'data', 'zmScore']
        zm_score = dict_parse.dict_parse(data, key_list, 3)

        return zm_score

    def get_tdscore(self,data):
        key_list=['tdReport','data','final_score']
        td_score=dict_parse.dict_parse(data,key_list,3)

        return td_score

    def get_contact(self,data):
        key_list=['partyInfo','data','partyMobileContacts','mobileContacts']
        contact=dict_parse.dict_parse(data,key_list,4)

        if contact=='None':
            num=0
        else:
            num=len(contact)

        return num



'''
xx=get_lrds_maindoc()
table=mongo_additionalinfo()
for row in xx.find().sort('crtTime',-1).limit(10):
    print(table.get_applyid(row),table.get_contactscore(row))
'''