
# coding: utf-8

# In[ ]:

from __future__ import division
from numpy.random import randn
from pandas import Series
import numpy as np
import pandas as pd
import os
import time;
import datetime  
import redis
np.set_printoptions(precision=4)
import sys; 
#%pwd


# In[ ]:

config_parms = {
    'minute_file_path' : 'E:/project/pychram/traderesp/base/output-csv/work/everyday/',
    'minute_file_name' : 'minute.txt',
    'redis_host' : 'localhost',
    'redis_port' : 6379,
    'redis_db'   : 0,
    # 加载全量数据
    # input CSV 基本 stock 数据文件目录(到20151218为止)，不包括之后每天的数据文件
    'input_base_stock_data_path' : 'E:/project/pychram/traderesp/base/input-csv/from-begin-to-20151218-day-stock/',
    #每日的 stock CSV 数据文件目录(20151218之后的文件)
    'input_everyday_stock_data_path' : 'E:/project/pychram/traderesp/base/input-csv/everyday-index-stock/',
    #每日的 stock CSV 数据文件名称
    'input_everyday_stock_data_name' : 'stock overview.csv',
    #退市股票代码名称
    'input_ts_stock_code_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-stock-ema12-ema26-macd/ts_stock.csv',
    # Base One file name
    'output_global_base_stock_data_onefile_name':'E:/onefile_global_all_base_stock.csv',
    # Every One file name
    'output_everyday_stock_data_onefile_name':'E:/onefile_all_everyday_stock.csv',
}


# In[ ]:

# 得到当前时间串
def get_cur_day():
    cur_day = datetime.datetime.now() 
    cur_day_str = cur_day.strftime('%Y-%m-%d'); 
    cur_day_trim_str = cur_day.strftime('%Y%m%d');
    cur_minute = cur_day.strftime('%Y-%m-%d %H:%M:%S'); 
    return cur_day,cur_day_str,cur_day_trim_str,cur_minute


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
        print (i,file_name)
        # 从csv文件中读取该股票数据 
        # 注意：这里请填写数据文件在您电脑中的路径
        stock_data = pd.read_csv(file_name,
                                 parse_dates=[2],encoding='gbk',converters={u'股票代码':str},usecols=[u'股票代码',u'交易日期', u'收盘价'])
                                 #parse_dates=[2],encoding='gbk',nrows=5,usecols=[u'交易日期', u'股票代码', u'收盘价'])
        # print stock_data.columns
        # 选取需要的字段，去除其他不需要的字段
        # 股票代码,股票名称,交易日期,新浪行业,新浪概念,新浪地域,开盘价,最高价,最低价,收盘价,后复权价,前复权价,涨跌幅,成交量,成交额,换手率,流通市值,总市值,是否涨停,是否跌停,市盈率TTM,市销率TTM,市现率TTM,市净率,MA_5,MA_10,MA_20,MA_30,MA_60,MA金叉死叉,MACD_DIF,MACD_DEA,MACD_MACD,MACD_金叉死叉,KDJ_K,KDJ_D,KDJ_J,KDJ_金叉死叉,布林线中轨,布林线上轨,布林线下轨,psy,psyma,rsi1,rsi2,rsi3
        stock_data.columns = ['code','date','close']
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
def get_base_stock_code_list(stock_base_csv_path):
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

# 计算 STOCK 的 macd 数据，包括新的和旧的
def get_from_begin_to_today_stock(stock_base_csv_path):
    #old stock code list
    stock_code_list = get_base_stock_code_list(stock_base_csv_path)
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
        print (i,code)
        # 从csv文件中读取该股票数据 
        # 注意：这里请填写数据文件在您电脑中的路径
        stock_data = pd.read_csv(stock_base_csv_path + code + '.csv',
                                 #parse_dates=[2],encoding='gbk',nrows=5,converters={u'股票代码':str},usecols=[u'股票代码',u'交易日期', u'收盘价'])
                                 parse_dates=[2],encoding='gbk',converters={u'股票代码':str},usecols=[u'股票代码',u'交易日期', u'收盘价'])
        # print stock_data.columns
        # 选取需要的字段，去除其他不需要的字段
        # 股票代码,股票名称,交易日期,新浪行业,新浪概念,新浪地域,开盘价,最高价,最低价,收盘价,后复权价,前复权价,涨跌幅,成交量,成交额,换手率,流通市值,总市值,是否涨停,是否跌停,市盈率TTM,市销率TTM,市现率TTM,市净率,MA_5,MA_10,MA_20,MA_30,MA_60,MA金叉死叉,MACD_DIF,MACD_DEA,MACD_MACD,MACD_金叉死叉,KDJ_K,KDJ_D,KDJ_J,KDJ_金叉死叉,布林线中轨,布林线上轨,布林线下轨,psy,psyma,rsi1,rsi2,rsi3
        stock_data.columns = ['code','date','close']
        # 去掉 stock 代表市场的前两个字符
        trim_stock_code = stock_data['code'].map(lambda x: x[2:])
        stock_data['code'] = trim_stock_code
        #
        # 将该股票的合并到output中
        all_stock = all_stock.append(stock_data,ignore_index=True)
    return all_stock


# In[ ]:

# only global init use.
def get_all_base_stock_data_to_one_file(stock_base_csv_path,out_file):
    #
    base_stock = get_from_begin_to_today_stock(stock_base_csv_path)
    base_stock.to_csv(out_file)
    return True


# In[ ]:

# only everyday first run use.
def get_one_file_base_stock_and_everyday_data_and_save_to_onefile(base_onefile,out_onefile,
                                                                 stock_everyday_csv_path,stock_everyday_csv_name,
                                                                 ts_stock_code_file_name):
    #
    global G_ALL_STOCK
    #
    everyday_stock = get_every_days_stock_data(stock_everyday_csv_path,stock_everyday_csv_name)
    base_stock = pd.read_csv(base_onefile,encoding='gbk',converters={'code':str},dtype={'code':str},usecols=['code','date','close'])
    #
    G_ALL_STOCK = everyday_stock.append(base_stock,ignore_index=True)
    
    # 去除已经退市的股票
    ts_stock_code = pd.read_csv(ts_stock_code_file_name,names=['code'],dtype={'code':str})
    G_ALL_STOCK = G_ALL_STOCK[G_ALL_STOCK['code'].isin(ts_stock_code['code']) == False]
    #
    G_ALL_STOCK.to_csv(out_onefile)
    return True


# In[ ]:

def get_today_pd_stock(base_onefile,out_onefile,stock_everyday_csv_path,stock_everyday_csv_name,ts_stock_code_file_name):
    global G_ALL_STOCK
    global G_DAY_STR
    #
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    if cur_day_str == G_DAY_STR:
        return "Not need init."
    else:
        G_DAY_STR = cur_day_str
        #
        print ("begin:",cur_minute)
        get_one_file_base_stock_and_everyday_data_and_save_to_onefile(base_onefile,
                                                                      out_onefile,
                                                                      stock_everyday_csv_path,stock_everyday_csv_name,
                                                                      ts_stock_code_file_name)
        #
        cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
        print ("end:",cur_minute)
        return "Need init. now OK."


# In[ ]:

#从 Redis 中加载当前数据
def load_minute_data_from_redis(redis_host,redis_port,redis_db):
    r = redis.StrictRedis(host=redis_host,port=redis_port,db=redis_db)
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    cur_minute_price = r.hgetall('day:' + cur_day_trim_str)
    cur_minute_price_dict = {str(key) : (float(cur_minute_price[key]) / 10000) if int(cur_minute_price[key]) != 0 else np.nan for key in cur_minute_price }
    cur_minute_price_series = pd.Series(cur_minute_price_dict)
    cur_minute_stock_dataframe = pd.DataFrame(cur_minute_price_series)
    cur_minute_stock_dataframe.columns=['close']
    cur_minute_stock_dataframe = cur_minute_stock_dataframe.dropna()
    #
    cur_minute_stock_dataframe.insert(0,'date',datetime.datetime.now().strftime('%Y-%m-%d'))
    trim_stock_code = cur_minute_stock_dataframe.index.map(lambda x: str(x)[2:-1])
    cur_minute_stock_dataframe.insert(0,'code',trim_stock_code)
    #
    return cur_minute_stock_dataframe


# In[ ]:

def calc_macd_data(minute_data):
    global G_ALL_STOCK
    now_stock = G_ALL_STOCK.append(minute_data,ignore_index=True)
    macd_stock =  pd.DataFrame()
    num=0
    for code,grouped in now_stock.groupby('code'):
        #排序
        stock_data = grouped.sort_values(by='date', ascending=True,inplace=False)
        #
        close = stock_data['close']
        ema_12 = close.ewm(span=12).mean()
        ema_26 = close.ewm(span=26).mean()
        diff = ema_12 - ema_26
        dea = diff.ewm(span=9).mean()
        macd = 2*(diff - dea)
        # 将该股票的合并到output中
        data_dirc = {'code':code,'diff':diff.tail(1).iloc[0],'dea':dea.tail(1).iloc[0],'macd':macd.tail(1).iloc[0]}
        data_series = pd.Series(data_dirc)
        macd_stock = macd_stock.append(data_series,ignore_index=True )
        #
        num=num+1
        if num > 10:
            #break
            pass
    #
    cur_macd_num = macd_stock[macd_stock['macd'] > 0.0].count()['code']
    cur_zhcd_num = macd_stock[(macd_stock['diff'] > 0.0) & (macd_stock['dea'] > 0.0) & (macd_stock['macd'] > 0.0)].count()['code']
    return cur_macd_num,cur_zhcd_num,macd_stock


# In[ ]:

# 计算 MACD 数量并写入文件
def write_macd_num_to_file(num1,num2,minute_file_path,minute_file_name):
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    output_file_dir = minute_file_path + cur_day_str;
    print (output_file_dir)
    if os.path.isdir(output_file_dir) == False:
        os.mkdir(output_file_dir)  
    output_file_file_name = output_file_dir + "/" + minute_file_name
    with open(output_file_file_name,'a') as handle:
        if int(num1) == 0 or int(num2) == 0 : 
            pass
        else:
            handle.writelines(str(cur_minute) + "," + str(num1) + "," + str(num2) + "\n")
            handle.close()
    


# In[ ]:

def reload_today_onefile_stock_data_for_debug(file_name):
    global G_ALL_STOCK
    G_ALL_STOCK = pd.read_csv(file_name,encoding='gbk',converters={'code':str},dtype={'code':str},usecols=['code','date','close'])
    #
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    G_DAY_STR = cur_day_str
    return True


# In[ ]:

def check_stock_cur_macd_for_debug(code='300342',date='2016-05-27',close=27.52):
    global G_ALL_STOCK
    judge = G_ALL_STOCK[G_ALL_STOCK['code']==code].sort_values(by='date', ascending=True,inplace=False)
    judge_1 = judge.append(pd.Series({'code':code,'date':date,'close':close}),ignore_index=True)
    close = judge_1['close']
    ema_12 = close.ewm(span=12).mean()
    ema_26 = close.ewm(span=26).mean()
    diff = ema_12 - ema_26
    dea = diff.ewm(span=9).mean()
    macd = 2*(diff - dea)
    judge_1['diff'] = diff
    judge_1['dea'] = dea
    judge_1['macd'] = macd
    return judge_1


# In[ ]:

# only debug use
#reload_today_onefile_stock_data_for_debug(config_parms['output_everyday_stock_data_onefile_name'])


# In[ ]:

# only global run once. if onefile base lost, then run it
#get_all_base_stock_data_to_one_file(config_parms["input_base_stock_data_path"],config_parms["output_global_base_stock_data_onefile_name"])


# In[ ]:

#300342
#judge_ret = check_stock_cur_macd_for_debug(code='300342',date='2016-05-27',close=27.52)


# In[ ]:

def run(conf):
    while True:
        tm = datetime.datetime.now()
        abeg = datetime.datetime(tm.year,tm.month,tm.day,9,20,0)
        aend = datetime.datetime(tm.year,tm.month,tm.day,11,30,0)
        bbeg = datetime.datetime(tm.year,tm.month,tm.day,13,0,0)
        bend = datetime.datetime(tm.year,tm.month,tm.day,15,0,0)
        if (tm > abeg and tm < aend) or (tm > bbeg and tm < bend) :
        #if (True):
            ret_str = get_today_pd_stock(conf['output_global_base_stock_data_onefile_name'],
                       conf['output_everyday_stock_data_onefile_name'],               
                       conf['input_everyday_stock_data_path'],
                       conf['input_everyday_stock_data_name'],
                       conf['input_ts_stock_code_file_name'])
            print (ret_str)

            tm = datetime.datetime.now()
            minute_data = load_minute_data_from_redis(conf['redis_host'],conf['redis_port'],conf['redis_db']);
            cur_diff_num,cur_dea_num,macd_stock = calc_macd_data(minute_data)
            print (tm,cur_diff_num,cur_dea_num)
            write_macd_num_to_file( cur_diff_num,cur_dea_num,conf['minute_file_path'],conf['minute_file_name'])
        else:
            print (tm,"market closed.")
        time.sleep(60)


# In[ ]:

G_ALL_STOCK = pd.DataFrame()
G_DAY_STR = ""


# In[ ]:

run(config_parms)


# In[ ]:




# In[ ]:



