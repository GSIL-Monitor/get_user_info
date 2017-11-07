#!/usr/bin/python
# encoding=utf-8


from get_user_info.config import init_app
from ti_daf import SqlTemplate,sql_util
import pandas as pd
from datetime import datetime
import logging
import time



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
    logger = logging.getLogger(__name__)
    starttime = time.time()
    logger.info('to get cif_loantime begin')

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

    endtime = time.time()
    logger.info('end of query data fromTime=[%s], toTime=[%s].' % (starttime, endtime))

    return loantime_df



def get_cdss_txntime():
    init_app()
    logger = logging.getLogger(__name__)
    starttime = time.time()
    logger.info('to get cdss_txntime begin')

    sql=''' select  distinct txnpartyid,min(txntime),max(txntime)
            from dev_dw.f_txnlist
            where txnflag='S' and salesamt>0 
            group by txnpartyid
    '''

    sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/oracle/dev_dw_db')

    time_list=[]
    for row in sql_row:
        logger.info('loop ing')
        time_list.append(list(row))

    logger.info('loop end')
    txntime_df=pd.DataFrame(time_list,columns=['partyid','firsttime','lasttime'])

    endtime = time.time()
    logger.info('end of query data fromTime=[%s], toTime=[%s].' % (starttime, endtime))

    return txntime_df


