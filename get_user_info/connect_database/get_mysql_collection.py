#!/usr/bin/python
# encoding=utf-8


from get_user_info.config import init_app
from ti_daf import SqlTemplate,sql_util
import pandas as pd
from datetime import datetime
import logging
import time

def get_psns_call():

    init_app()

    sql = 'select  a.partyid,count(distinct b.idcallee) count_contact from Caller a left join Callee b ' \
          'on a.idcaller=b.idcaller' \
          ' group by a.partyid'

    sql_row = sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/mysql/ac_psns_db')

    contact_list=[]
    for row in sql_row:
        contact_list.append(list(row))

    contact_df=pd.DataFrame(contact_list,columns=['partyid','contacts'])
    return contact_df



def get_cif_M2():

    init_app()
    logger = logging.getLogger(__name__)
    starttime=time.time()

    sql='''   select distinct partyid,
              case when y.pid is not null then 'M2' else 'NM' end categroy
              from  dev_dw.f_loanagreement x
              left join 
              (
                select distinct a.partyid pid from  dev_dw.f_loanagreement a
                left join  dev_dw.f_loanrepayschedule b
                on a.id=b.idloanagreement
                where b.repaytime-b.duedate>33 or (b.repaytime is null and sysdate-b.duedate>33)
              )y
              on x.partyid=y.pid
              where loantime<to_date('2017-09-01','yyyy-mm-dd') '''

    sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/oracle/dev_dw_db')

    partyid_list=[]
    for row in sql_row:
        partyid_list.append(list(row))

    endtime = time.time()
    logger.info('end of query data fromTime=[%s], toTime=[%s].' % (starttime, endtime))

    partyid_df=pd.DataFrame(partyid_list,columns=['partyid','status'])

    return partyid_df


def get_cif_loantime():
    init_app()

    sql='''
    select  partyid,max(loantime)  from dev_db.f_loanagreement 
    where loanstatus in ('D','O','R','E')
    group by partyid
    '''

    sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/oracle/dev_dw_db')

    loantime_list=[]
    for row in sql_row:
        loantime_list.append(list(row))

    loantime_df=pd.DataFrame(loantime_list,columns=['partyid','loantime'])

    return loantime_df



def get_cdss_txntime():

    init_app()

    sql=''' select  distinct partyid,firstAcqTxnTime,lastestAcqTxnTime
            from ac_cdss_db.PartyStatistic
    '''

    sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/mysql/ac_cif_db')

    time_list=[]
    for row in sql_row:
        time_list.append(list(row))

    txntime_df=pd.DataFrame(time_list,columns=['partyid','firsttime','lasttime'])

    return txntime_df

