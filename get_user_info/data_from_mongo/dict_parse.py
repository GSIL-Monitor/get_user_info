#!/usr/bin/python
# encoding=utf-8

def dict_parse(var_dict,key_ls,layer):
#var_dict 为传入字典 key_ls 为访问路径的key列表 ，layer 表示层数 ，支持2-4层字典解析

    if layer==2:

        if key_ls[0] not in var_dict.keys():
            value='None'
        elif key_ls[1] not in var_dict[key_ls[0]].keys():
            value='None'
        else:
            value=var_dict[key_ls[0]][key_ls[1]]

        return value



    if layer==3:

        if key_ls[0] not in var_dict.keys():
            value='None'
        elif key_ls[1] not in var_dict[key_ls[0]].keys():
            value='None'
        elif key_ls[2] not in var_dict[key_ls[0]][key_ls[1]].keys():
            value='None'
        else:
            value = var_dict[key_ls[0]][key_ls[1]][key_ls[2]]

        return value


    if layer==4:

        if key_ls[0] not in var_dict.keys():
            value='None'
        elif key_ls[1] not in var_dict[key_ls[0]].keys():
            value='None'
        elif key_ls[2] not in var_dict[key_ls[0]][key_ls[1]].keys():
            value='None'
        elif key_ls[3] not in var_dict[key_ls[0]][key_ls[1]][key_ls[2]].keys():
            value='None'
        else:
            value=var_dict[key_ls[0]][key_ls[1]][key_ls[2]][key_ls[3]]

        return value


