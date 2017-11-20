#!/usr/bin/python
# encoding=utf-8


from get_user_info.data_from_mongo import dict_parse
from get_user_info.connect_database import get_mongo_collection


class mongo_pcrinfo():

#pcr baseinfo

    def get_pcr_baseinfo(self,return_para):
        key_list=['pcrReport', 'data', 'baseInfo']
        mid_dict=dict_parse.dict_parse(self,key_list,len(key_list))

        key_name = ['maritalStatus', 'creditCardNum', 'overduedCounts', 'exceedNinetyDaysCounts','maxMonthsOverdue',
                    'haveBadDebts', 'totalCreditLine', 'totalCreditLineUsed', 'loanFreq', 'totalLoanAmount',
                    'totalLoanBalance', 'cceRate']

        count_list = []
        for key in key_name:
            if mid_dict =='None' or mid_dict is None or key not in mid_dict.keys():
                count_list.append('None')
            else:
                value = mid_dict[key]
                count_list.append(value)

        if return_para == 'name':
            return key_name
        elif return_para == 'value':
            return count_list


    def get_pcr_creditcardstatus(self,return_para):
        key_list = ['pcrReport', 'data', 'creditCardStats']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))

        key_name = ['cardCount', 'badDebtCardCount', 'overDue90DayCardCount','overDueUpToNowCardCount',
                    'totalMonthsOverdue','overDueUpToNowAmount', 'validCNYCreditCardCount','validCNYCreditCardMaxCreditLine',
                    'validCNYCreditCardTotalCreditLine', 'validCNYCreditCardTotalCreditLineUsed','validCNYCreditCardCreditUsageRate',
                    'validCNYCreditCardMinOpenDate', 'validCNYCreditCardMaxOpenMonthSpan']

        count_list = []
        for key in key_name:
            if mid_dict =='None' or mid_dict is None or key not in mid_dict.keys():
                count_list.append('None')
            else:
                value = mid_dict[key]
                count_list.append(value)

        if return_para == 'name':
            return key_name
        elif return_para == 'value':
            return count_list



    def get_pcr_loanstatus(self,return_para):
        key_list = ['pcrReport', 'data', 'loanStats']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))

        key_name = ['overDueUpToNowBizLoanAmount', 'overDueUpToNowLoanCount','overDueUpToNowOtherLoanAmount', 'overDue90DayBizLoanCount',
                    'overDueUpToNowCarLoanAmount', 'overDue90DayOtherLoanCount', 'overDueUpToNowHousingLoanCount',
                    'overDue90DayHousingLoanCount', 'overDueUpToNowCarLoanCount','overDueUpToNowHomeLoanCount', 'badDebtLoanCount',
                    'overDueUpToNowHomeLoanAmount', 'overDueUpToNowOtherLoanCount','overDue90DayHomeLoanCount', 'totalMonthsOverdue',
                    'overDueUpToNowHousingLoanAmount', 'loanCount', 'overDue90DayLoanCount',
                    'overDue90DayCarLoanCount','overDueUpToNowBizLoanCount']

        count_list = []
        for key in key_name:
            if  mid_dict =='None' or mid_dict is None or key not in mid_dict.keys() :
                count_list.append('None')
            else:
                value = mid_dict[key]
                count_list.append(value)

        if return_para == 'name':
            return key_name
        elif return_para == 'value':
            return count_list



    def get_pcr_accesstatus(self,return_para):
        key_list = ['pcrReport', 'data', 'accessStats']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))

        key_name = ['selfInqInLast3m', 'auditInqInLast3m', 'selfInqInLast6m', 'accessCount',
                    'counterQueryCount', 'selfInqViaCounterInLast6m']

        count_list = []
        for key in key_name:
            if mid_dict =='None' or mid_dict is None or key not in mid_dict.keys():
                count_list.append('None')
            else:
                value = mid_dict[key]
                count_list.append(value)

        if return_para == 'name':
            return key_name
        elif return_para == 'value':
            return count_list



