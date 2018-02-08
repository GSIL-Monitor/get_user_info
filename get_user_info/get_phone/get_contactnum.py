#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo.dict_parse import dict_parse
from get_user_info.connect_database.get_mongo_collection import get_psns_phonecontact
import pandas as pd
from get_user_info.config import init_app
from get_user_info.data_merge.send_email import EmailSend
import os

path=os.path.dirname(__file__)


def get_contact():
    init_app()

    psns_contact=get_psns_phonecontact()

    contact_list=[]
    for item in psns_contact.find():
        partyid=item['partyId']
        num=len(item['items'])

        contact_list.append([partyid,num])

    contact_df=pd.DataFrame(contact_list,columns=['partyid','phone_num'])

    return contact_df




def email_task():
    init_app()

    score_df=get_contact()
    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/phone.xlsx',engine='xlsxwriter')
    score_df.to_excel(excel_writer,index=False)
    excel_writer.save()

    subject = 'relative_phone'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'relative_phone'
    attachment_file = "/home/andpay/data/excel/phone.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])


