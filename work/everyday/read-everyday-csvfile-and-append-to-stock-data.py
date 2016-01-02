
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
#sys.path.append('book_scripts')
#%pwd


# In[ ]:

config_parms = {
    # input CSV 基本 stock 数据文件目录(到20151218为止)，不包括之后每天的数据文件
    #'input_base_stock_data_path' : 'E:/project/pychram/traderesp/base/input-csv/2013-2014-day-stock-history-test/',
    'input_base_stock_data_path' : 'E:/project/pychram/traderesp/base/input-csv/from-begin-to-20151218-day-stock/',
    #每日的 stock CSV 数据文件目录(20151218之后的文件)
    'input_everyday_stock_data_path' : 'E:/project/pychram/traderesp/base/input-csv/everyday-index-stock/',
    #每日的 stock CSV 数据文件名称
    'input_everyday_stock_data_name' : 'stock overview.csv',
    'output_macd_num_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-stock-ema12-ema26-macd/today_stock_macd.txt',
    'output_stock_data_cvs_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-stock-ema12-ema26-macd/end-is-today-stock-ema12-ema26-macd.csv'
}


# In[ ]:

# 得到每日的 STOCK CSV 文件名列表
def get_every_days_stock_files(stock_everyday_csv_path,stock_everyday_csv_name):
    abs_filename_list = []
    for root, dirs, files in os.walk(stock_everyday_csv_path):# 注意：这里请填写数据文件在您电脑中的路径
        if dirs:
            for item in dirs:
                abs_filename_list.append(stock_everyday_csv_path + item + '/' + stock_everyday_csv_name)
    return abs_filename_list


# In[ ]:

#加载每日的 STOCK CSV 文件
def get_every_days_stock_data(stock_everyday_csv_path,stock_everyday_csv_name):
    file_list = get_every_days_stock_files(stock_everyday_csv_path,stock_everyday_csv_name)
    #print file_list
    all_stock = pd.DataFrame()
    i=0
    # 遍历每个股票
    for file_name in file_list:
        # 测试 5 次跳过
        i+=1
        if i>= 5:
            #break
            pass
        print file_name
        # 从csv文件中读取该股票数据 
        # 注意：这里请填写数据文件在您电脑中的路径
        stock_data = pd.read_csv(file_name,
                                 parse_dates=[2],encoding='gbk')
        # print stock_data.columns
        # 选取需要的字段，去除其他不需要的字段
        # 股票代码,股票名称,交易日期,新浪行业,新浪概念,新浪地域,开盘价,最高价,最低价,收盘价,后复权价,前复权价,涨跌幅,成交量,成交额,换手率,流通市值,总市值,是否涨停,是否跌停,市盈率TTM,市销率TTM,市现率TTM,市净率,MA_5,MA_10,MA_20,MA_30,MA_60,MA金叉死叉,MACD_DIF,MACD_DEA,MACD_MACD,MACD_金叉死叉,KDJ_K,KDJ_D,KDJ_J,KDJ_金叉死叉,布林线中轨,布林线上轨,布林线下轨,psy,psyma,rsi1,rsi2,rsi3
        stock_data = stock_data[[ u'交易日期',u'股票代码', u'收盘价','MACD_DIF','MACD_DEA','MACD_MACD']]
        stock_data.columns = ['date','code','close','macd_dif','macd_dea','macd_macd']
        # 去掉 stock 代表市场的前两个字符
        trim_stock_code = stock_data['code'].map(lambda x: x[2:])
        stock_data['code'] = trim_stock_code
        #
        # 将该股票的合并到output中
        all_stock = all_stock.append(stock_data, ignore_index=True)
    #
    return all_stock


# In[ ]:

# 得到初始的 STOCK CSV 文件名列表
def get_all_stock_code_list(stock_base_csv_path):
    # ========== 遍历数据文件夹中所有股票文件的文件名，得到股票代码列表stock_code_list
    stock_code_list = []
    for root, dirs, files in os.walk(stock_base_csv_path):# 注意：这里请填写数据文件在您电脑中的路径
        if files:
            for f in files:
                if '.csv' in f:
                    stock_code_list.append(f.split('.csv')[0])
    #
    return stock_code_list


# In[ ]:

# 从每日数据中得到新的 STOCK 数据(去除掉昨天之前已经有的)
def get_new_stock(every_days_stock,stock_code_list):
    stock_code_base = [code[2:] for code in stock_code_list]
    code_not_new = every_days_stock.ix[:,'code'].isin(stock_code_base)
    code_not_new_need_del = code_not_new[code_not_new == True]
    new_stock_data = every_days_stock.drop(code_not_new_need_del.index)
    return new_stock_data


# In[ ]:

# 计算新 STOCK 的 macd 数据
def gen_new_stock_macd_data(new_stock_data):
    all_new_stock = pd.DataFrame()
    #
    new_stock_dict = dict(list(new_stock_data.groupby('code')))
    for code in new_stock_dict:
        print code
        stock_data = new_stock_dict[code]
        #排序
        stock_data.sort_values(by='date', ascending=True,inplace=True)
        #
        ema_12 = pd.ewma(stock_data['close'], span=12)
        ema_26 = pd.ewma(stock_data['close'], span=26)
        stock_data['ema_12'] = ema_12
        stock_data['ema_26'] = ema_26
        stock_data['diff'] = ema_12 - ema_26
        stock_data['dea'] = pd.ewma(stock_data['diff'], span=9)
        stock_data['macd'] = 2*(stock_data['diff'] - stock_data['dea'])
        # 将该股票的合并到output中
        all_new_stock = all_new_stock.append(stock_data.tail(1), ignore_index=True)
    #
    return all_new_stock


# In[ ]:

# 计算 STOCK 的 macd 数据，包括新的和旧的
def gen_from_begin_to_today_stock_macd_data(stock_base_csv_path,stock_everyday_csv_path,stock_everyday_csv_name):
    #加载每天的数据
    every_days_stock = get_every_days_stock_data(stock_everyday_csv_path,stock_everyday_csv_name)
    #old stock code list
    stock_code_list = get_all_stock_code_list(stock_base_csv_path)
    # ========== 根据上一步得到的代码列表，遍历所有股票，将这些股票合并到一张表格 all_stock 中
    all_stock = pd.DataFrame()
    i=0
    # 遍历每个股票
    for code in stock_code_list:
        # 测试 5 次跳过
        i+=1
        if i>= 5:
            #break
            pass
        print code
        # 从csv文件中读取该股票数据 
        # 注意：这里请填写数据文件在您电脑中的路径
        stock_data = pd.read_csv(stock_base_csv_path + code + '.csv',
                                 parse_dates=[2],encoding='gbk')
        # print stock_data.columns
        # 选取需要的字段，去除其他不需要的字段
        # 股票代码,股票名称,交易日期,新浪行业,新浪概念,新浪地域,开盘价,最高价,最低价,收盘价,后复权价,前复权价,涨跌幅,成交量,成交额,换手率,流通市值,总市值,是否涨停,是否跌停,市盈率TTM,市销率TTM,市现率TTM,市净率,MA_5,MA_10,MA_20,MA_30,MA_60,MA金叉死叉,MACD_DIF,MACD_DEA,MACD_MACD,MACD_金叉死叉,KDJ_K,KDJ_D,KDJ_J,KDJ_金叉死叉,布林线中轨,布林线上轨,布林线下轨,psy,psyma,rsi1,rsi2,rsi3
        stock_data = stock_data[[ u'交易日期',u'股票代码', u'收盘价','MACD_DIF','MACD_DEA','MACD_MACD']]
        stock_data.columns = ['date','code','close','macd_dif','macd_dea','macd_macd']
        # 去掉 stock 代表市场的前两个字符
        trim_stock_code = stock_data['code'].map(lambda x: x[2:])
        stock_data['code'] = trim_stock_code
        #加入 every day 的数据
        cur_code_every_data = every_days_stock[every_days_stock['code'] == code[2:]]
        stock_data = stock_data.append(cur_code_every_data, ignore_index=True)
        #排序
        stock_data.sort_values(by='date', ascending=True,inplace=True)
        #
        ema_12 = pd.ewma(stock_data['close'], span=12)
        ema_26 = pd.ewma(stock_data['close'], span=26)
        stock_data['ema_12'] = ema_12
        stock_data['ema_26'] = ema_26
        stock_data['diff'] = ema_12 - ema_26
        stock_data['dea'] = pd.ewma(stock_data['diff'], span=9)
        stock_data['macd'] = 2*(stock_data['diff'] - stock_data['dea'])
        #
        # 将该股票的合并到output中
        all_stock = all_stock.append(stock_data.tail(1), ignore_index=True)
    #新的stock
    new_stock_data = get_new_stock(every_days_stock,stock_code_list)
    all_new_stock = gen_new_stock_macd_data(new_stock_data)
    #
    # 将 新股票的合并到output中
    all_stock = all_stock.append(all_new_stock, ignore_index=True)
    #
    return all_stock


# In[ ]:

# 得到最后一天的 MACD 数量
def get_today_macd_nums_and_write_to_file(all_stock,macd_num_file_name):
    #cur_day = datetime.datetime.now() 
    #cur_day_str = cur_day.strftime('%Y-%m-%d'); 
    #today_stock = all_stock[all_stock['date'] == cur_day_str]
    lastday = all_stock['date'].max()
    lastday_stock = all_stock[all_stock['date'] == lastday]
    
    lastday_macd_num = lastday_stock[lastday_stock['macd_macd'] > 0.0].count()['code']
    lastday_zhcd_num = lastday_stock[(lastday_stock['macd_dif'] > 0.0) & (lastday_stock['macd_dea'] > 0.0) & (lastday_stock['macd_macd'] > 0.0)].count()['code']
    # ========== 将算好的数据输出到文件 - 注意：这里请填写输出文件在您电脑中的路径
    handle = open(macd_num_file_name,"a")
    handle.write(str(lastday) + "," + str(lastday_macd_num) + "," + str(lastday_zhcd_num) + "\n")
    handle.close()
    return lastday,lastday_macd_num,lastday_zhcd_num


# In[ ]:

def run(conf):
    all_stock = gen_from_begin_to_today_stock_macd_data(conf['input_base_stock_data_path'],conf['input_everyday_stock_data_path'],conf['input_everyday_stock_data_name'])
    lastday,lastday_macd_num,lastday_zhcd_num = get_today_macd_nums_and_write_to_file(all_stock,conf['output_macd_num_file_name'])
    print lastday,lastday_macd_num,lastday_zhcd_num
    # 按 date , code 排序
    output = all_stock.sort_values(by=['date','code'],ascending=True)
    output
    # ========== 将算好的数据输出到csv文件 - 注意：这里请填写输出文件在您电脑中的路径
    # output CSV数据文件
    output.to_csv(conf['output_stock_data_cvs_file_name'],encoding='gbk', index=False)    


# In[ ]:

run(config_parms)


# In[ ]:



