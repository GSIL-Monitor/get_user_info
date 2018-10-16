#!/usr/bin/python
# encoding=utf-8

from ti_config.bootstrap import init_ti_srv_cfg
from ti_daf.sql_context import SqlContext, session_scope, iselect_rows_by_sql
from ti_daf import SqlTemplate,sql_util
import json
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
from get_user_info.data_merge.send_email import EmailSend
import os
from get_user_info.config import init_app

init_app()

today=datetime.date(datetime.today())
yesterday=today-dt.timedelta(days=1)
afterday=today+dt.timedelta(days=2)


sql='''
select to_char(b.crttime,'yyyy-mm-dd') days,b.servicestaff,a.source,
count(distinct b.customerid) call_num,
count(distinct case when b.connected=1 then b.customerid end ) connect_num,
count(distinct case when b.valid=1  then b.customerid end) valid_num,
count(distinct case when c.partyid is not null then b.customerid end) login_num,
count(distinct case when d.partyid is not null then b.customerid end) loan_num
from  dev_dw.f_customer a
left join  dev_dw.f_callinfo b
on a.id=b.customerid
left join 
(
    select  distinct partyid  from  dev_dw.f_loginhistory
    where  logintime>=to_date('''+"'"+str(yesterday)+"'" +''','yyyy-mm-dd')
    and logintime<to_date('''+"'"+str(today)+"'"+''','yyyy-mm-dd')
) c
on a.partyid=c.partyid
left join
(
    select  distinct partyid  from dev_dw.f_loanagreement 
    where  idproduct<>21 and loanstatus in ('D','O','R','E')
    and loantime>=to_date('''+"'"+str(yesterday)+"'" +''','yyyy-mm-dd')
    and loantime<to_date('''+"'"+str(afterday)+"'"+''','yyyy-mm-dd')
) d
on a.partyid=d.partyid
left join 
(
    select distinct partyid from dev_dw.f_loanapplyinfo
    where idproduct<>21 
    and applydate>=to_date('''+"'"+str(yesterday)+"'" +''','yyyy-mm-dd')
    and applydate<to_date('''+"'"+str(afterday)+"'" +''','yyyy-mm-dd')
) e
on a.partyid=e.partyid
where  a.source  in  ('未续借信用通商户','未提现信用通商户')
and b.crttime>=to_date('''+"'"+str(yesterday)+"'" +''','yyyy-mm-dd')
and  b.crttime<to_date('''+"'"+str(afterday)+"'"+''','yyyy-mm-dd')
group by to_char(b.crttime,'yyyy-mm-dd'),b.servicestaff,a.source
'''

sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/python/db/dev_dw_db',max_size=-1)

data_list=[]
for row in sql_row:
    data_list.append(list(row))

data_df=pd.DataFrame(data_list,columns=['day','services','source','call_num','connect_num','valid_num','login_num','loan_num'])
data_df=data_df.sort_values(by=['day','services'],ascending=[0,1])

no_withdraw_df=data_df[data_df['source']=='未提现信用通商户']
no_renew_df=data_df[data_df['source']=='未续借信用通商户']
no_withdraw_df=no_withdraw_df.drop(['source'],axis=1)
no_renew_df=no_renew_df.drop(['source'],axis=1)

login_rate_withdraw=no_withdraw_df['login_num']/no_withdraw_df['valid_num']
login_rate_renew=no_renew_df['login_num']/no_renew_df['valid_num']

no_withdraw_df.insert(6,'login_rate',login_rate_withdraw)
no_renew_df.insert(6,'login_rate',login_rate_renew)

no_withdraw_df.columns=['触达日期','触达经理','触达人数','接通人数','有效人数','登入人数','登入率','提现人数']
no_renew_df.columns=['触达日期','触达经理','触达人数','接通人数','有效人数','登入人数','登入率','提现人数']


excel_writer=pd.ExcelWriter('/data/excel/service_feedback_report.xlsx')
work_book=excel_writer.book
#这里可自行添加excel的设置格式
format_1=work_book.add_format({'align':'center','font_name':'微软雅黑'})
format_2=work_book.add_format({'align':'center','num_format':'0.0%','font_name':'微软雅黑'})

no_withdraw_df.to_excel(excel_writer,'未提现',index=False)
no_withdraw_sheet=excel_writer.sheets['未提现']
no_withdraw_sheet.set_column('A:Z',16,format_1)
no_withdraw_sheet.set_column('G:G',16,format_2)

no_renew_df.to_excel(excel_writer,'未续借',index=False)
no_renew_sheet=excel_writer.sheets['未续借']
no_renew_sheet.set_column('A:Z',16,format_1)
no_renew_sheet.set_column('G:G',16,format_2)

excel_writer.save()


subject = '客服触达反馈日报表'
#to_addrs = ['sha.wu@andpay.me','lulu.zhang@andpay.me','qian.yuan@andpay.me','kesheng.wang@andpay.me']
to_addrs = ['kesheng.wang@andpay.me']
body_text = '未触达+未提现'
attachment_file = "/data/excel/service_feedback_report.xlsx"

EmailSend.send_email(subject, to_addrs, body_text,attachment_files=[attachment_file] )
