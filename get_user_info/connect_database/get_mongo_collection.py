#!/usr/bin/python
# encoding=utf-8

from ti_config import bootstrap
from ti_daf import MongoTemplate
from get_user_info.config import init_app



def get_lrds_maindoc():
    init_app()

    mongodb_path_lrds = bootstrap.ti_config_service.get_value('get_user_info.mongodb_path_lrds', default='/Users/andpay/PycharmProjects/get_user_info/ac_lrds_db')
    mongodb_name_lrds = bootstrap.ti_config_service.get_value('get_user_info.mongodb_name_lrds', default='ac_lrds_db')
    db = MongoTemplate.get_database(mongodb_path_lrds, mongodb_name_lrds)

    #print(db.collection_names(include_system_collections=False))  查询数据库中的表
    #['mybankLoanMainDoc', 'mainDoc', '_asyncDataHandlerRegisters']  所有表

    collection = db.get_collection("mainDoc")

    return  collection



def get_cif_partyadditioninfo():
    init_app()

    mongodb_path_cif = bootstrap.ti_config_service.get_value('get_user_info.mongodb_path_cif',
                                                         default='/Users/andpay/PycharmProjects/score_card_end/ac_cif_db')

    mongodb_name_cif = bootstrap.ti_config_service.get_value('get_user_info.mongodb_name_cif', default='ad_cif_db')
    db = MongoTemplate.get_database(mongodb_path_cif, mongodb_name_cif)
    #print(db.collection_names(include_system_collections=False))  查询数据库中的表

    collection = db.get_collection("PartyAdditionInfo")

    return collection


def get_psns_phonecontact():
    init_app()
    mongodb_path_psns = bootstrap.ti_config_service.get_value('get_user_info.mongodb_path_psns',
                                                         default='/Users/andpay/PycharmProjects/score_card_end/ac_psns_db')

    mongodb_name_psns = bootstrap.ti_config_service.get_value('get_user_info.mongodb_name_psns', default='ac_psns_db')
    db = MongoTemplate.get_database(mongodb_path_psns, mongodb_name_psns)
    #print(db.collection_names(include_system_collections=False))  查询数据库中的表

    collection = db.get_collection("phoneContacts")

    return collection
