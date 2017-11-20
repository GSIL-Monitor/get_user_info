#!/usr/bin/python
# encoding=utf-8

from get_user_info.data_from_mongo import dict_parse


class mongo_phoneinfo():

    def get_devicequery(self):
        key_list=['mobileDevices', 'data', 'deviceIdQuery','count']
        value=dict_parse.dict_parse(self,key_list,len(key_list))

        return value


    def get_activeuserquery(self):
        key_list = ['mobileDevices', 'data', 'activateUserNameQuery', 'count']
        value = dict_parse.dict_parse(self, key_list, len(key_list))

        return value


    def get_applist_byusername(self,return_para):
        key_list = ['mobileAppList', 'data', 'byApplyUsername']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))

        key_name=['deviceIds', 'deviceDataIds', 'appNames']
        if return_para=='name':
            return key_name

        elif return_para=='value':
            count_list=[]
            for key in key_name:
                if mid_dict =='None' or mid_dict is None:
                    count_list.append('None')
                else:
                    value=len(mid_dict[key])
                    count_list.append(value)

            return count_list


    def get_applist_bydevice(self, return_para):
        key_list = ['mobileAppList', 'data', 'byApplyDevice']
        mid_dict = dict_parse.dict_parse(self, key_list, len(key_list))

        key_name = ['deviceDataIds', 'appNames', 'deviceIds']
        if return_para == 'name':
            return key_name

        elif return_para == 'value':
            count_list = []
            for key in key_name:
                if mid_dict =='None' or mid_dict is None:
                    count_list.append('None')
                else:
                    value = len(mid_dict[key])
                    count_list.append(value)

            return count_list


    def get_succphonecall(self,return_para):
        key_list=['mobileCallLog', 'data']
        mid_dict=dict_parse.dict_parse(self,key_list,len(key_list))

        first_layer=['succPhoneCallStats','callStatsIn6m','r008CallStatsIn3m','r009CallStatsIn3m','r010CallStatsIn3m']


        if return_para == 'name':

            key_name_list = []
            for item in first_layer:
                key_name=['callCount','totalCallDuration']
                for key in key_name:
                    end_name=item+'_'+key
                    key_name_list.append(end_name)

            return key_name_list


        elif return_para == 'value':

            count_list = []
            for item in first_layer:
                if mid_dict =='None' or mid_dict is None:
                    count_list=['None','None']
                else:
                    key_name=['callCount','totalCallDuration']
                    for key in key_name:
                        if item not in mid_dict.keys() or mid_dict[item] is None  :
                            count_list.append('None')
                        else:
                            value = mid_dict[item][key]
                            count_list.append(value)

            return count_list



    def get_contact(self):
        key_list = ['partyInfo', 'data', 'partyMobileContacts', 'mobileContacts']
        contact = dict_parse.dict_parse(self, key_list, len(key_list))

        if contact is None or contact == 'None':
            num = 0
        else:
            num = len(contact)

        return num

