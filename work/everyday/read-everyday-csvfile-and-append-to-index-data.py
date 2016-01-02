
# coding: utf-8

# In[ ]:

from __future__ import division
from numpy.random import randn
from pandas import Series
import numpy as np
import pandas as pd
import os
import datetime  
np.set_printoptions(precision=4)
import sys; 


# In[ ]:

config_parms = {
    # input CSV 基本指数数据文件目录(到20151218为止)，不包括之后每天的数据文件
    'input_base_index_data_path' :  'E:/project/pychram/traderesp/base/input-csv/from-begin-to-20151218-day-index/',
    #每日的指数 CSV 数据文件目录(20151218之后的文件)
    'input_everyday_index_data_path' : 'E:/project/pychram/traderesp/base/input-csv/everyday-index-stock/',
    #每日的指数 CSV 数据文件名称
    'input_everyday_index_data_name' : 'index data.csv',
    #计算 M26 值的 指数的名称
    'm26_index_name' : 'sh000001',
    'output_m26_singnal_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-index-m26/today_index_m26.txt',
    'output_index_data_cvs_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-index-m26/end-is-today-index-m26.csv'
}


# In[ ]:

# 得到每日的指数 CSV 文件名列表
def get_every_days_index_files(index_everyday_csv_path,index_everyday_csv_name):
    abs_filename_list = []
    for root, dirs, files in os.walk(index_everyday_csv_path):# 注意：这里请填写数据文件在您电脑中的路径
        if dirs:
            for item in dirs:
                abs_filename_list.append(index_everyday_csv_path + item + '/' + index_everyday_csv_name)
    return abs_filename_list


# In[ ]:

#加载每日的指数 CSV 文件
def get_every_days_index_data(index_everyday_csv_path,index_everyday_csv_name):
    file_list = get_every_days_index_files(index_everyday_csv_path,index_everyday_csv_name)
    #print file_list
    all_index = pd.DataFrame()
    i=0
    # 遍历每个股票
    for file_name in file_list:
        # 测试 5 次跳过
        i+=1
        if i>= 5:
            #break
            pass
        print file_name
        # 从csv文件中读取该指数数据 
        # 注意：这里请填写数据文件在您电脑中的路径
        index_data = pd.read_csv(file_name,
                                 parse_dates=['date'],encoding='gbk')
        # print index_data.columns
        # 选取需要的字段，去除其他不需要的字段
        # index_code,date,open,close,low,high,volume,money,change
        index_data = index_data[['index_code','date','close']]
        #
        # 将该股票的合并到output中
        all_index = all_index.append(index_data, ignore_index=True)
    #
    return all_index


# In[ ]:

# 得到初始的指数 CSV 文件名列表
def get_all_index_code_list(index_base_csv_path):
    # ========== 遍历数据文件夹中所有指数文件的文件名，得到指数代码列表index_code_list
    index_code_list = []
    for root, dirs, files in os.walk(index_base_csv_path):# 注意：这里请填写数据文件在您电脑中的路径
        if files:
            for f in files:
                if '.csv' in f:
                    index_code_list.append(f.split('.csv')[0])
    #
    return index_code_list


# In[ ]:

# 生成 指数的 M26 数据
def gen_from_begin_to_today_m26_data(index_base_csv_path,index_everyday_csv_path,index_everyday_csv_name):
    #加载每天的数据
    every_days_index = get_every_days_index_data(index_everyday_csv_path,index_everyday_csv_name)
    #print every_days_index
    #old index code list
    index_code_list = get_all_index_code_list(index_base_csv_path)
    # ========== 根据上一步得到的代码列表，遍历所有指数，将这些指数合并到一张表格 all_index 中
    all_index = pd.DataFrame()
    i=0
    # 遍历每个指数
    for index_code in index_code_list:
        # 测试 5 次跳过
        i+=1
        if i>= 5:
            #break
            pass
        print index_code
        # 从csv文件中读取该股票数据 
        # 注意：这里请填写数据文件在您电脑中的路径
        index_data = pd.read_csv(index_base_csv_path + index_code + '.csv',
                                 parse_dates=['date'],encoding='gbk')
        # print index_data.columns
        # 选取需要的字段，去除其他不需要的字段
        # index_code,date,open,close,low,high,volume,money,change
        index_data = index_data[[ 'date','index_code', 'close']]
        #加入 every day 的数据
        cur_index_code_every_data = every_days_index[every_days_index['index_code'] == index_code]
        index_data = index_data.append(cur_index_code_every_data, ignore_index=True)
        #排序
        index_data.sort_values(by='date', ascending=True,inplace=True)
        # 只计算 26 日的移动平均线
        ma_26 = 26
        # 计算简单算术移动平均线MA - 注意：index_data['close']为指数每天的收盘点
        index_data['MA_26'] = pd.rolling_mean(index_data['close'], ma_26)
        # 判断是否中轨之上
        # 当当天的【close】> M26 时，将【收盘发出的信号】设定为1
        buy_index = index_data[index_data['close'] > index_data['MA_26']].index
        index_data.loc[buy_index, 'close_signal'] = 1
        # 当当天的【close】< M26 时，将【收盘发出的信号】设定为 0
        sell_index = index_data[index_data['close'] < index_data['MA_26']].index
        index_data.loc[sell_index, 'close_signal'] = 0
        # 计算每天的仓位，当天应该买入时，【当天的买卖信号】为1，当天不应该买入时，【当天的买卖信号】为0
        index_data['today_signal'] = index_data['close_signal'].shift(1)
        #
        # 将该指数合并到output中
        all_index = all_index.append(index_data, ignore_index=True)
    #遍历完成，返回全部
    return all_index


# In[ ]:

def get_today_index_m26_singnal_and_write_to_file(all_index,m26_index_code,m26_signal_file_name):
    sh_index = all_index[(all_index['index_code'] == m26_index_code) ]
    sh_index = sh_index.tail(1)
    last_day_str = sh_index['date'].values[0]
    last_index_m26_flag = sh_index['today_signal'].values[0]
    # ========== 将算好的数据输出到文件 - 注意：这里请填写输出文件在您电脑中的路径
    handle = open(m26_signal_file_name,"a")
    handle.write(str(last_day_str)[0:10] + "," + str(last_index_m26_flag) + "\n")
    handle.close()
    return last_day_str,last_index_m26_flag


# In[ ]:

def run(conf):
    all_index = gen_from_begin_to_today_m26_data(conf['input_base_index_data_path'],conf['input_everyday_index_data_path'],conf['input_everyday_index_data_name'])
    last_day_str,last_index_m26_flag = get_today_index_m26_singnal_and_write_to_file(all_index,conf['m26_index_name'],conf['output_m26_singnal_file_name'])
    print last_day_str,last_index_m26_flag
    # 按 date , code 排序
    output = all_index.sort_values(by=['date','index_code'],ascending=True)
    #output
    # ========== 将算好的数据输出到csv文件 - 注意：这里请填写输出文件在您电脑中的路径
    # output CSV数据文件
    output.to_csv(conf['output_index_data_cvs_file_name'],encoding='gbk', index=False)


# In[ ]:

run(config_parms)


# In[ ]:



