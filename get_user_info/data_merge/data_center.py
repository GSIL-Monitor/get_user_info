#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo.mongo_basicinfo import mongo_basicinfo
from get_user_info.data_from_mongo.mongo_pcrinfo import mongo_pcrinfo
from get_user_info.data_from_mongo.mongo_phoneinfo import mongo_phoneinfo
from get_user_info.data_from_mongo.mongo_txninfo import mongo_txninfo
from get_user_info.data_from_mongo.mongo_additionalinfo import mongo_additionalinfo
from get_user_info.connect_database.get_mongo_collection import get_lrds_maindoc
from get_user_info.data_from_mongo.mongo_basicinfo import get_file



def data_center(data,return_para):
    if return_para == 'name':
        basic_name = merge_basic_info(data, 'name')
        pcr_name = merge_pcr_info(data, 'name')
        phone_name = merge_phone_info(data, 'name')
        txn_name = merge_txn_info(data, 'name')
        additional_name = merge_additional_info(data, 'name')

        key_name = basic_name + pcr_name + phone_name + txn_name + additional_name

        return key_name

    else:
        applyid=mongo_basicinfo.get_applyid(data)
        if list(applyid)[0]!='T':
            basic_value=merge_basic_info(data,'value')
            pcr_value=merge_pcr_info(data,'value')
            phone_value=merge_phone_info(data,'value')
            txn_value=merge_txn_info(data,'value')
            additional_value=merge_additional_info(data,'value')

            key_value=basic_value+pcr_value+phone_value+txn_value+additional_value

            return key_value


def merge_basic_info(data,return_para):

    if return_para == 'name':
        key_name=['partyid','applyid','phone','age','gender','marriage','city']

        return key_name

    elif return_para == 'value':
        basic_info_value=[]
        basic_info_value.append(mongo_basicinfo.get_partyid(data))
        basic_info_value.append(mongo_basicinfo.get_applyid(data))
        basic_info_value.append(mongo_basicinfo.get_phone(data))
        basic_info_value.append(mongo_basicinfo.get_age(data))
        basic_info_value.append(mongo_basicinfo.get_gender(data))
        basic_info_value.append(mongo_basicinfo.get_marr(data))
        basic_info_value.append(mongo_basicinfo.get_city(get_file(),data=data))

        return basic_info_value


def merge_pcr_info(data,return_para):

    if return_para == 'name':
        baseinfo_name=mongo_pcrinfo.get_pcr_baseinfo(data,'name')
        creditcard_name=mongo_pcrinfo.get_pcr_creditcardstatus(data,'name')
        loan_name=mongo_pcrinfo.get_pcr_loanstatus(data,'name')
        access_name=mongo_pcrinfo.get_pcr_accesstatus(data,'name')

        key_name=baseinfo_name+creditcard_name+loan_name+access_name

        return key_name

    elif return_para == 'value':
        baseinfo_value=mongo_pcrinfo.get_pcr_baseinfo(data,'value')
        creditcard_value=mongo_pcrinfo.get_pcr_creditcardstatus(data,'value')
        loan_value=mongo_pcrinfo.get_pcr_loanstatus(data,'value')
        access_value=mongo_pcrinfo.get_pcr_accesstatus(data,'value')

        key_value=baseinfo_value+creditcard_value+loan_value+access_value

        return key_value


def rename(item_list,key_word):

    rename_list=[]
    for name in item_list:
        new_name=key_word+'_'+name
        rename_list.append(new_name)

    return rename_list


def merge_phone_info(data,return_para):

    if return_para == 'name':

        query_name=['contacts','devicequery','activeuserquery']

        user_name=mongo_phoneinfo.get_applist_byusername(data,'name')
        end_user_name=rename(user_name,'user')
        device_name=mongo_phoneinfo.get_applist_bydevice(data,'name')
        end_device_name=rename(device_name,'device')
        phonecall_name=mongo_phoneinfo.get_succphonecall(data,'name')

        end_key_name=query_name+end_user_name+end_device_name+phonecall_name

        return end_key_name

    elif return_para == 'value':
        query_list=[]
        query_list.append(mongo_phoneinfo.get_contact(data))
        query_list.append(mongo_phoneinfo.get_devicequery(data))
        query_list.append(mongo_phoneinfo.get_activeuserquery(data))

        user_value=mongo_phoneinfo.get_applist_byusername(data,'value')
        device_value=mongo_phoneinfo.get_applist_bydevice(data,'value')
        phonecall_value=mongo_phoneinfo.get_succphonecall(data,'value')

        end_key_value=query_list+user_value+device_value+phonecall_value

        return end_key_value


def merge_txn_info(data,return_para):

    if return_para == 'name':
        dispute_name=['distupetxn']
        txn3m_name=mongo_txninfo.get_txn3m(data,'name')
        end_txn3m_name=rename(txn3m_name,'txn3m')
        txn6m_name=mongo_txninfo.get_txn6m(data,'name')
        end_txn6m_name=rename(txn6m_name,'txn6m')

        end_key_name=dispute_name+end_txn3m_name+end_txn6m_name
        return end_key_name

    elif return_para == 'value':

        dispute_value=[mongo_txninfo.get_disputetxn(data)]
        txn3m_value=mongo_txninfo.get_txn3m(data,'value')
        txn6m_value=mongo_txninfo.get_txn6m(data,'value')

        end_key_value=dispute_value+txn3m_value+txn6m_value
        return end_key_value


def merge_additional_info(data,return_para):
    if return_para == 'name':
        key_name=['zmscore','zmatfscore','zmwatchlist','zmrisklist','tdscore','blackbehavior','qhrskscore']

        return key_name

    elif return_para == 'value':
        value_list=[]
        value_list.append(mongo_additionalinfo.get_zmscore(data))
        value_list.append(mongo_additionalinfo.get_zmatfscore(data))
        value_list.append(mongo_additionalinfo.get_zmwatchlist(data))
        value_list.append(mongo_additionalinfo.get_zmrisklist(data))
        value_list.append(mongo_additionalinfo.get_tdscore(data))
        value_list.append(mongo_additionalinfo.get_brackbehavior(data))
        value_list.append(mongo_additionalinfo.get_qhrskscore(data))

        return value_list

