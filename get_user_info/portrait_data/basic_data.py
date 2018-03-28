from get_user_info.config import init_app
from ti_daf import SqlTemplate,sql_util
import pandas as pd
import time
from ti_config.bootstrap import init_ti_srv_cfg
from datetime import datetime
import datetime as dt
import numpy as np


def get_cif_loantime():

    init_app()

    sql='''
    select  partyid,max(loantime)  from dev_db.f_loanagreement 
    where loanstatus in ('D','O','R','E')
    group by partyid
    '''

    sql_row=sql_util.select_rows_by_zsql(sql_text=sql,sql_paras={},ns_server_id='/db/oracle/dev_dw_db')

    loantime_list=[]
    for row in sql_row:
        print(row)
        loantime_list.append(list(row))

    loantime_df=pd.DataFrame(loantime_list,columns=['partyid','loantime'])

    return loantime_df

get_cif_loantime()