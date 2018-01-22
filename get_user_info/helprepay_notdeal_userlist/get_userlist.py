#!/usr/bin/python
# encoding=utf-8

from get_user_info.config import init_app
from ti_daf import SqlTemplate, sql_util, TxMode
import pandas as pd
from datetime import datetime
from get_user_info.data_merge.send_email import EmailSend
import datetime as dt
from ti_lnk.ti_lnk_client import TiLnkClient
from ti_daf.sql_context import SqlContext, session_scope, iselect_rows_by_sql


def get_week_day(date):
    week_day_dict = {
        0: '星期一',
        1: '星期二',
        2: '星期三',
        3: '星期四',
        4: '星期五',
        5: '星期六',
        6: '星期天',
    }
    day = date.weekday()
    return week_day_dict[day]


def get_mobile_phone(partyid):
    init_app()

    sql = '''
            select ao.corporateRepresentUserName phoneNumber from ac_cif_db.OrgParty ao
            where ao.partyId = :partyId
        '''
    db = sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/python/db/ac_cif_db', max_size=-1)
    param = dict()
    param['partyId'] = partyid
    row_list = db.query_record(sql, params=param)
    phone_num = None
    for row in row_list:
        phone_num = row['phoneNumber']

    if phone_num is None:
        return 0

    service_group = 'ac-ums.admin-srv'
    service_id = 'me.andpay.ac.ums.api.UserManagementService'
    user = TiLnkClient.call_lnk_srv(service_group, service_id, 'getUserByUserName', phone_num, ns_server_id=None)

    return user['userName']



def nodeal_user():

    init_app()

    today = datetime.date(datetime.today())
    today_s="'"+str(today)+"'"

    week_day = get_week_day(today)
    week_list = ['星期二', '星期三', '星期四', '星期五']


    if week_day in week_list:
        time = today - dt.timedelta(days=1)
    elif week_day=='星期一':
        time = today - dt.timedelta(days=3)

    time_s = "'" + str(time) + "'"

    sql=''' select  a.partyid,a.applyinfoid,a.repaymode,b.status,b.hasrepayamt
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            where  a.applystatus<>'O' and a.applytime>='''+time_s+''' 
            and a.applytime<'''+today_s


    sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/mysql/ac_bts_db',max_size=-1)

    partyid_list=[]
    for row in sql_row:
        partyid_list.append(list(row))

    partyid_df=pd.DataFrame(partyid_list,columns=['partyid','applyid','repaymode','status','hasrepayamt'])

    return partyid_df


def data_merge(user_df):
    user_list=list(user_df['partyid'])

    phone_list=[]
    for partyid in user_list:
        phone_number=get_mobile_phone(partyid)
        phone_list.append([partyid,phone_number])

    phone_df=pd.DataFrame(phone_list,columns=['partyid','phone_number'])

    end_df=pd.merge(user_df,phone_df,on='partyid',how='left')

    return end_df



def email_task():

    nodeal_df=nodeal_user()
    res_df = data_merge(nodeal_df)

    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/helprepay_nodeal_userlist.xlsx',engine='xlsxwriter')
    res_df.to_excel(excel_writer,index=False)
    excel_writer.save()


    subject = 'helprepay_nodeal_userlist'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'helprepay_nodeal_userlist'
    attachment_file = "/home/andpay/data/excel/helprepay_nodeal_userlist.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])
