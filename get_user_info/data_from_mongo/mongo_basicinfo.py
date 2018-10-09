#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo import dict_parse
from get_user_info.connect_database import get_mongo_collection
import pandas as pd
import datetime as dt
import os


def get_file():
    path = os.path.dirname(__file__)
    path = path + '/city.xlsx'
    city_file = pd.read_excel(path)
    return city_file

class mongo_basicinfo():


    def get_age(self):
        key_list = ['loanApplyInfo', 'data', 'certNo']
        certno = dict_parse.dict_parse(self, key_list, 3)

        if len(certno) == 18:
            born_date = certno[6:10]
            now_date = dt.datetime.now().strftime('%Y')
            age = int(now_date) - int(born_date)
        else:
            age = 'None'

        return age

    def get_gender(self):
        key_list = ['loanApplyInfo', 'data', 'certNo']
        certno = dict_parse.dict_parse(self, key_list, 3)

        if len(certno) == 18:
            gender = certno[16:17]
            gender_num = int(gender) % 2
        else:
            gender_num = 'None'

        return gender_num

    def get_marr(self):
        key_list = ['loanApplyInfo', 'data', 'marriaged']
        marr = dict_parse.dict_parse(self, key_list, 3)

        return marr

    def get_city(self,data):
        key_list = ['loanApplyInfo', 'data', 'certNo']
        certno = dict_parse.dict_parse(data, key_list, 3)

        if len(certno) == 18:
            no = certno[0:4]

            city_list = []
            for cityno in self['city_no']:
                if str(cityno)[0:4] == no:
                    var_city = self[self['city_no'] == cityno]['city'].values[0]
                    city_list.append(var_city)

            if len(city_list) > 0:
                city = city_list[0]
            else:
                city = 'None'

        else:
            city = 'None'

        return city

    def get_partyid(self):
        key_list = ['loanApplyInfo', 'data', 'partyId']
        partyid = dict_parse.dict_parse(self, key_list, 3)

        return partyid

    def get_phone(self):
        key_list = ['loanApplyInfo', 'data', 'mobile']
        phone = dict_parse.dict_parse(self, key_list, 3)

        return phone

    def get_applyid(self):
        applyid=self['applyId']

        return applyid