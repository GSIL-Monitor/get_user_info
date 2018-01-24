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

    sql=''' select  *  from
            (
                select pid,ids,repaymode,
                case when  applystatus='O' and cancelflag=TRUE then '用户终止结清'
                     when  applystatus='O' and fallback=1  then '扣款失败结清'
                     when  applystatus='O' then '正常结清'
                     when  applystatus='C' and cancelflag=TRUE then '用户终止撤销'
                     when  applystatus='C' then '扣款失败撤销'
                     else  '未结清' end  deal_status
                from  
                (
                    select  a.applyinfoid ids,a.partyid pid,a.applystatus,a.repaymode,
                            json_extract(a.applydata,'$.issuerName') bank,
                            json_extract(txndata,'$.customLine') ctline,
                            json_extract(txndata,'$.userCancelFlag') cancelflag,
                            b.fallback,b.status,b.repayamt,b.payedamt,b.hasrepayamt
                    from ac_bts_db.ApplyInfo a
                    left join ac_bts_db.InsteadRepayTxnCtrl b
                    on a.applyinfoid=b.applyinfoid
                    where applytime>='2018-01-22' and applytime<'2018-01-24'
                ) x
            ) y
            where deal_status in ('用户终止结清','用户终止撤销','扣款失败撤销')
        '''


    sql_row=sql_util.select_rows_by_sql(sql_text=sql,sql_paras={},ns_server_id='/db/mysql/ac_bts_db',max_size=-1)

    partyid_list=[]
    for row in sql_row:
        partyid_list.append(list(row))

    partyid_df=pd.DataFrame(partyid_list,columns=['partyid','applyid','repaymode','status'])

    return partyid_df

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

    nodeal_df=nodeal_user()
    res_df = data_merge(nodeal_df)
    res_df = res_df.drop_duplicates(subset=['partyid', 'applyid', 'repaymode', 'status','phone_number'])

    excel_writer=pd.ExcelWriter('/home/andpay/data/excel/helprepay_nodeal_userlist.xlsx',engine='xlsxwriter')
    res_df.to_excel(excel_writer,index=False)
    excel_writer.save()


    subject = 'helprepay_nodeal_userlist'
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'helprepay_nodeal_userlist'
    attachment_file = "/home/andpay/data/excel/helprepay_nodeal_userlist.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])
