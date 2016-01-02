
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
    'input_stock_macd_data_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-stock-ema12-ema26-macd/end-is-today-stock-ema12-ema26-macd.csv',
    'minute_file_path' : 'E:/project/pychram/traderesp/base/output-csv/work/everyday/',
    'minute_file_name' : 'minute.txt',
    'redis_host' : 'localhost',
    'redis_port' : 6379,
    'redis_db'   : 0
    
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

# 计算 MACD 数量并写入文件
def write_macd_num_to_file(num1,num2,minute_file_path,minute_file_name):
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    output_file_dir = minute_file_path + cur_day_str;
    print output_file_dir
    if os.path.isdir(output_file_dir) == False:
        os.mkdir(output_file_dir)  
    output_file_file_name = output_file_dir + "/" + minute_file_name
    with open(output_file_file_name,'a') as handle:
        handle.writelines(str(cur_minute) + "," + str(num1) + "," + str(num2) + "\n")
        handle.close()
    


# In[ ]:

# 加载 prev day 数据
def load_prev_days_macds_data(macd_csv_file):
    stock_data = pd.read_csv(macd_csv_file,parse_dates=[0],encoding='gbk')
    stock_data = stock_data[['date','code','ema_12','ema_26','dea']]
    return stock_data


# In[ ]:

#从 Redis 中加载当前数据
def load_minute_data_from_redis(redis_host,redis_port,redis_db):
    r = redis.StrictRedis(host=redis_host,port=redis_port,db=redis_db)
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    cur_minute_price = r.hgetall('day:' + cur_day_trim_str)
    cur_minute_price_dict = {int(key) : (float(cur_minute_price[key]) / 10000) if int(cur_minute_price[key]) != 0 else np.nan for key in cur_minute_price }
    cur_minute_price_series = pd.Series(cur_minute_price_dict)
    cur_minute_stock_dataframe = pd.DataFrame(cur_minute_price_series)
    cur_minute_stock_dataframe.columns=['price']
    cur_minute_stock_dataframe = cur_minute_stock_dataframe.dropna()
    #
    return cur_minute_stock_dataframe


# In[ ]:

def calc_macd_data(last_data,minute_data):
    bing_stock = pd.merge(last_data,minute_data,left_on='code',right_index=True)
    bing_stock = bing_stock.dropna()
    #EMA（12）= 前一日EMA（12）×11/13＋今日收盘价×2/13
    #EMA（26）= 前一日EMA（26）×25/27＋今日收盘价×2/27
    #DIFF=今日EMA（12）- 今日EMA（26）
    #DEA（MACD）= 前一日DEA×8/10＋今日DIF×2/10 
    #BAR=2×(DIFF－DEA)
    bing_stock['ema_cur_12'] = bing_stock['ema_12'] * 11/13 + bing_stock['price']*2/13
    bing_stock['ema_cur_26'] = bing_stock['ema_26'] * 25/27 + bing_stock['price']*2/27
    bing_stock['cur_diff'] = bing_stock['ema_cur_12'] - bing_stock['ema_cur_26']
    bing_stock['cur_dea'] = bing_stock['dea'] * 8/10 + bing_stock['cur_diff'] * 2/10
    bing_stock['cur_macd'] = 2*(bing_stock['cur_diff'] - bing_stock['cur_dea'])
    #
    cur_macd_num = bing_stock[bing_stock['cur_macd'] > 0.0].count()['code']
    
    cur_zhcd_num = bing_stock[(bing_stock['cur_diff'] > 0.0) & (bing_stock['cur_dea'] > 0.0) & (bing_stock['cur_macd'] > 0.0)].count()['code']
    print cur_zhcd_num
    return cur_macd_num,cur_zhcd_num


# In[ ]:

def run(conf):
    while True:
        tm = datetime.datetime.now()
        abeg = datetime.datetime(tm.year,tm.month,tm.day,9,30,0)
        aend = datetime.datetime(tm.year,tm.month,tm.day,11,30,0)
        bbeg = datetime.datetime(tm.year,tm.month,tm.day,13,0,0)
        bend = datetime.datetime(tm.year,tm.month,tm.day,15,0,0)
        if (tm > abeg and tm < aend) or (tm > bbeg and tm < bend) :
            last_data = load_prev_days_macds_data(conf['input_stock_macd_data_file_name']);
            minute_data = load_minute_data_from_redis(conf['redis_host'],conf['redis_port'],conf['redis_db']);
            cur_diff_num,cur_dea_num = calc_macd_data(last_data,minute_data)
            print tm,cur_diff_num,cur_dea_num
            write_macd_num_to_file( cur_diff_num,cur_dea_num,conf['minute_file_path'],conf['minute_file_name'])
        else:
            print tm,"market closed."
        time.sleep(60)


# In[ ]:

run(config_parms)


# In[ ]:



