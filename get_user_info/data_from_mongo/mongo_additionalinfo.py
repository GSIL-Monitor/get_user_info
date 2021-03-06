#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo import dict_parse
from get_user_info.connect_database.get_mongo_collection import get_lrds_maindoc
import pandas as pd
import datetime as dt



class mongo_additionalinfo():

    def get_zmscore(self):
        key_list = ['zmxyReport', 'data', 'zmScore']
        zm_score = dict_parse.dict_parse(self, key_list, 3)

        return zm_score


    def get_zmatfscore(self):
        key_list=['zmxyAntifraudScoreReport','data','score']
        zmatfscore=dict_parse.dict_parse(self,key_list,3)

        return zmatfscore


    def get_zmwatchlist(self):
        key_list=['zmxyWatchListReport','data','isMatched']
        watchlist=dict_parse.dict_parse(self,key_list,3)

        return watchlist


    def get_zmrisklist(self):
        key_list=['zmxyAntifraudRiskListReport','data','hit']
        risklist=dict_parse.dict_parse(self,key_list,3)

        return risklist


    def get_tdscore(self):
        key_list=['tdReport','data','final_score']
        td_score=dict_parse.dict_parse(self,key_list,3)

        return td_score


    def get_brackbehavior(self):
        key_list = ['partyInfo', 'data', 'partyBlackBehavior', 'count']
        value = dict_parse.dict_parse(self, key_list, len(key_list))

        return value


    def get_qhrskscore(self):
        key_list=['qhRskdooReport','data','rskScore']
        value=dict_parse.dict_parse(self,key_list,len(key_list))

        return value


'''
xx=get_lrds_maindoc()
table=mongo_additionalinfo()
for row in xx.find().sort('crtTime',-1).limit(10):
    print(table.get_applyid(row),table.get_contactscore(row))
'''