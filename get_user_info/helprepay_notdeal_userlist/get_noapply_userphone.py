from get_user_info.config import init_app
from ti_daf import SqlTemplate, sql_util, TxMode
import pandas as pd
from datetime import datetime
from get_user_info.data_merge.send_email import EmailSend
import datetime as dt
from ti_lnk.ti_lnk_client import TiLnkClient
from ti_daf.sql_context import  select_rows_by_sql
from ti_daf.sql_tx import session_scope


class DatabaseOperator():
    def __init__(self, ns_config):
        init_app()
        self.ns_server_id = ns_config

    # 根据sql_text查询记录
    def query_record(self, sql_text, return_type=None, params={}):
        with session_scope(tx_mode=TxMode.NONE_TX, ns_server_id=self.ns_server_id) as session:
            if return_type == 'List':
                self.row_result = select_rows_by_sql(sql_text, params, max_size=-1)
            else:
                self.row_result = session.execute(sql_text,params)

            return self.row_result
    '''
    # 删除主键对应的记录
    def delete_record(self, table_name, id):
        with session_scope(tx_mode=TxMode.NONE_TX, ns_server_id=self.ns_server_id) as session:
            delete_by_id(table_name, id)

    # 批量插入记录
    def batch_insert_record(self, table_name, insert_values):
        with session_scope(tx_mode=TxMode.NONE_TX, ns_server_id=self.ns_server_id) as session:
            batch_insert(table_name, insert_values)
    '''

    # 批量更新记录
    def batch_update_record(self, batch_update_values, table_name, id):
        with session_scope(tx_mode=TxMode.NONE_TX, ns_server_id=self.ns_server_id) as session:
            sql_util.batch_update(table_name, id, batch_update_values)



def nodeal_user():

    init_app()

    sql_1 = '''select distinct partyid   
            from ac_bts_db.ApplyInfo
          '''

    user_row = sql_util.select_rows_by_sql(sql_text=sql_1, sql_paras={}, ns_server_id='/python/db/ac_bts_db',
                                           max_size=-1)

    user_list = []
    for user in user_row:
        user_list.append(user[0])

    user_list = str(tuple(user_list))

    sql_2 = '''select distinct b.merchantCustomerId
            from  ac_agw_db.AuthBindCard a
            left join  ac_agw_db.MerchantUser b
            on a.merchantUserId=b.merchantUserId
            where a.authNetId  in ('08470009-00', '08470010-00')  and  a.status='1'  and a.crttime<'2018-01-30'
            and b.merchantCustomerId not in''' + user_list

    result_row = sql_util.select_rows_by_sql(sql_text=sql_2, sql_paras={}, ns_server_id='/python/db/ac_bts_db',
                                             max_size=-1)

    result_list = []
    for row in result_row:
        result_list.append(row[0])

    result_list1 = result_list[:700]
    result_list2 = result_list[700:]
    result_list1 = str(tuple(result_list1))
    result_list2 = str(tuple(result_list2))

    sql_3 = '''select  a.partyid,c.creditline,c.lineused,
                     d.loantimes,d.loanamt,e.txntimes,e.txnamt
            from dev_dw.dim_txnparty a
            left join dev_dw.bts_applyinfo b
            on a.partyid=b.partyid
            left join
            ( select x.partyid pid ,x.totalcreditline creditline,x.totalcreditlineused  lineused
              from dev_dw.f_pcrbasicinfo x
              join 
                  (
                    select partyid,max(id) mid  from dev_dw.f_pcrbasicinfo 
                    where reporttime>=to_date('2018-01-01','yyyy-mm-dd')
                    group by partyid
                  ) y
               on x.id=y.mid
            ) c
            on a.partyid=c.pid
            left join 
            (
            select partyid,count(distinct id) loantimes,sum(loanamt) loanamt  from dev_dw.f_loanagreement 
            where loantime >=to_date('2017-11-01','yyyy-mm-dd') and loanstatus in ('D','O','R','E')
            group by partyid
            ) d
            on a.partyid=d.partyid
            left join 
            (
            select  txnpartyid,count(distinct txnid) txntimes,sum(salesamt) txnamt  from dev_dw.f_txnlist 
            where txntime>=to_date('2017-11-01','yyyy-mm-dd') and txnflag='S' and salesamt>0
            group by txnpartyid
            ) e
            on a.partyid=e.txnpartyid 
            where a.partyid in ''' + result_list1 + ''' or a.partyid in ''' + result_list2

    oracle_row = sql_util.select_rows_by_sql(sql_text=sql_3, sql_paras={}, ns_server_id='/python/db/dev_dw_db',
                                             max_size=-1)

    user_info = []
    for row in oracle_row:
        user_info.append(list(row))

    col=['partyid', 'creditline', 'lineused', 'loantime_3m', 'loanamt_3m', 'txntime_3m', 'txnamt_3m']
    user_df = pd.DataFrame(user_info,columns=col)

    return user_df,col

def get_mobile_phone(partyId):
    init_app()

    db=DatabaseOperator('/python/db/ac_cif_db')

    sql = '''
            select ao.corporateRepresentUserName phoneNumber from ac_cif_db.OrgParty ao
            where ao.partyId = :partyId
        '''
    #db = sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/python/db/ac_cif_db', max_size=-1)
    param = dict()
    param['partyId'] = partyId
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

    nodeal_df,col=nodeal_user()
    res_df = data_merge(nodeal_df)
    col=col+['phone_number']
    res_df = res_df.drop_duplicates(subset=col)

    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/helprepay_nodeal_userlist.xlsx',engine='xlsxwriter')
    res_df.to_excel(excel_writer,index=False)
    excel_writer.save()


    subject = 'helprepay_nodeal_userlist'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'helprepay_nodeal_userlist'
    attachment_file = "/home/andpay/data/excel/helprepay_nodeal_userlist.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])
