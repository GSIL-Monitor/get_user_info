#!/usr/bin/python
# encoding=utf-8


from get_user_info.data_from_mongo import dict_parse
from get_user_info.connect_database import get_mongo_collection


class mongo_pcrinfo():

    def get_credit_num(self,data):
        key_list = ['pcrReport', 'data', 'creditCardStats', 'validCNYCreditCardCount']
        card_num = dict_parse.dict_parse(data, key_list, 4)

        return card_num

    def get_loan_num(self,data):
        key_list = ['pcrReport', 'data', 'pcrLoanRecords']
        loan_data = dict_parse.dict_parse(data, key_list, 3)

        loan_num = 0
        for item in loan_data:
            if item['loanType'] == '1' or item['loanType'] == '4':
                loan_num += 1

        return loan_num


    def get_creditcard_higest_quota(self,data):
        key_list = ['pcrReport', 'data', 'creditCardStats', 'validCNYCreditCardMaxCreditLine']
        higest_quota = dict_parse.dict_parse(data, key_list, 4)

        return higest_quota


    def get_history_overduenum(self,data):
        key_list = ['pcrReport', 'data', 'baseInfo', 'overduedCounts']
        overdue_num = dict_parse.dict_parse(data, key_list, 4)

        return overdue_num

    def get_creditcard_userate(self,data):
        key_list = ['pcrReport', 'data', 'creditCardStats', 'validCNYCreditCardCreditUsageRate']
        use_rate = dict_parse.dict_parse(data, key_list, 4)

        return use_rate

    def get_credit_inquiry(self,data):
        key_list = ['pcrReport', 'data', 'pcrAccessRecords']
        inquiry_data = dict_parse.dict_parse(data, key_list, 3)

        inquiry_num = 0

        if inquiry_data == 'None':
            inquiry_num = 'None'
        else:
            for item in inquiry_data:
                if item['accessReason'] != '贷后管理':
                    inquiry_num += 1

        return inquiry_num




