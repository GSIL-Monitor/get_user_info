#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo import dict_parse
from get_user_info.connect_database import get_mongo_collection
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