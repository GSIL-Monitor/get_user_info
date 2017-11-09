#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo.dict_parse import dict_parse
from get_user_info.connect_database.get_mongo_collection import get_cif_partyadditioninfo
import pandas as pd
from get_user_info.config import init_app
from get_user_info.data_merge.send_email import EmailSend
import os

path=os.path.dirname(__file__)
path=path+'/partyid.xlsx'
party_df=pd.read_excel(path)
party_list=party_df['PARTYID']


def get_relative(item):
    key_list=['contactsInfo','relative','mobile']
    relative_mobile=dict_parse(item,key_list,3)

    return relative_mobile


def get_colleague(item):
    key_list=['contactsInfo','colleague','mobile']
    colleague_mobile=dict_parse(item,key_list,3)

    return colleague_mobile


def get_relative_phone():

    init_app()

    table=get_cif_partyadditioninfo()

    phone_list=[]
    for item in table.find({'partyId':{'$in':party_list}}):

        partyid=item['partyId']
        relative_mobile=get_relative(item)
        colleague_moblie=get_colleague(item)


        ls=[partyid,relative_mobile,colleague_moblie]


        phone_list.append(ls)


    phone_df=pd.DataFrame(phone_list,columns=['partyid','relative_mobile','colleague_mobile'])

    return phone_df


def email_task():

    score_df=get_relative_phone()
    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/phone.xlsx',engine='xlsxwriter')
    score_df.to_excel(excel_writer,index=False)
    excel_writer.save()

    subject = 'relative_phone'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'relative_phone'
    attachment_file = "/home/andpay/data/excel/phone.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])
