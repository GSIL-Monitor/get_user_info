#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo import dict_parse


class mongo_txninfo():

    def get_disputetxn(self):
        key_list=['disputedTxn','data','disputedTxn6m','count']
        value=dict_parse.dict_parse(self,key_list,len(key_list))

        return value


    def get_txn3m(self,return_para):
        key_list = ['txnData3m', 'data']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))


        key_name=['debitQueryCount', 'totalQueryCount', 'creditCount', 'succPurchaseCount', 'totalTxnCount', 'succTxnCount',
                  'failedPurchaseCount', 'noBalanceCreditPurchaseCount', 'creditPurchaseAmts', 'creditPurchaseCount']

        if return_para=='name':
            return key_name

        elif return_para=='value':
            count_list=[]
            for key in key_name:
                if mid_dict =='None' or mid_dict is None:
                    count_list.append('None')
                else:
                    value=mid_dict[key]
                    count_list.append(value)

            return count_list


    def get_txn6m(self,return_para):
        key_list = ['txnData6m', 'data']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))

        key_name=['debitQueryCount', 'totalQueryCount', 'creditCount', 'succPurchaseCount', 'totalTxnCount', 'succTxnCount',
                  'failedPurchaseCount', 'noBalanceCreditPurchaseCount', 'creditPurchaseAmts', 'creditPurchaseCount']

        if return_para=='name':
            return key_name

        elif return_para == 'value':
            count_list=[]
            for key in key_name:
                if mid_dict =='None' or mid_dict is None:
                    count_list.append('None')
                else:
                    value=mid_dict[key]
                    count_list.append(value)

            return count_list
