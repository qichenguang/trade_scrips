
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
import easyquotation
import traceback
import easyquotation.helpers as helpers


# In[ ]:

config_parms = {
    'easyquotation_save_file_path' : 'thefile.csv',
    'data_source': 'sina',
    'redis_host' : 'localhost',
    'redis_port' : 6379,
    'redis_db'   : 0
}


# In[ ]:

def get_left_stock_data(first_stock_rows,quotation):
    all_stock_codes = helpers.get_stock_codes(realtime=True)
    code_series = Series(all_stock_codes,index=all_stock_codes)
    left_code = code_series[code_series.index.isin(first_stock_rows.index) == False]
    need_code = []
    for id in left_code:
        code = str(id)
        #print (code)
        if len(code) == 6 and (int(code[0]) == 6 or (code[0]) == "0" or int(code[0]) == 3):
            pass
        else:
            #print (code)
            continue 

        if int(code[0:3]) == 399 :
            #print (code)
            continue
        #
        need_code.append(code)
    #
    need_code_ret = quotation.stocks(need_code)
    ret_left = [ code for code in need_code if code not in need_code_ret ]
    
    need_stock_pd = pd.DataFrame(need_code_ret)
    need_stock_rows = need_stock_pd.T 
    return need_stock_rows,ret_left


# In[ ]:

def get_easyquotation_stock_data(source='sina'):
    try:
        #
        quotation = easyquotation.use(source) # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
        #
        first_stock_array = quotation.all
        first_stock_pd = pd.DataFrame(first_stock_array)
        first_stock_rows = first_stock_pd.T 
        #
        need_stock_rows,ret_left = get_left_stock_data(first_stock_rows,quotation)
        #
        all_stock_rows = first_stock_rows.append(need_stock_rows)
        #
        all_price = all_stock_rows[['now']]
        #
        stock_price_dict = {}
        all_num = 0
        left_num = 0
        for id,row in all_price.iterrows():
            all_num += 1
            code = str(id)
            if len(code) == 6 and (int(code[0]) == 6 or (code[0]) == "0" or int(code[0]) == 3):
                pass
            else:
                #print "code error:", row['code'],row['shortname']
                continue 
  
            # 大于 1000 为指数
            if int(row['now']) > 1000:
                #print (" > 1000 error:", code,row['now'])
                continue    
            # 停牌
            if float(row['now']) < 0.1:
                #print (" < 0.1 error:", code,row['now'])
                #continue            
                pass

            #print row
            left_num += 1
            stock_price_dict[str(id)] = int(row['now'] * 10000.0)
        return all_num,left_num,ret_left,stock_price_dict,all_stock_rows
    except Exception:
        traceback.print_exc()
    return (False,False,False,None,None)


# In[ ]:

# 得到当前时间串
def get_cur_day():
    cur_day = datetime.datetime.now() 
    cur_day_str = cur_day.strftime('%Y-%m-%d'); 
    cur_day_trim_str = cur_day.strftime('%Y%m%d');
    cur_minute_str = cur_day.strftime('%Y-%m-%d-%H-%M-%S'); 
    return cur_day,cur_day_str,cur_day_trim_str,cur_minute_str


# In[ ]:

#从 Redis 中加载当前数据
def add_minute_data_to_redis(stock_price_dict,redis_host,redis_port,redis_db):
    r = redis.StrictRedis(host=redis_host,port=redis_port,db=redis_db)
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()

    result = r.hmset('day:' + cur_day_trim_str,stock_price_dict)

    return result


# In[ ]:

def run(conf):
    while True:
        tm = datetime.datetime.now()
        abeg = datetime.datetime(tm.year,tm.month,tm.day,9,30,1)
        aend = datetime.datetime(tm.year,tm.month,tm.day,11,30,0)
        bbeg = datetime.datetime(tm.year,tm.month,tm.day,13,0,0)
        bend = datetime.datetime(tm.year,tm.month,tm.day,15,0,0)
        if (tm > abeg and tm < aend) or (tm > bbeg and tm < bend) :
        #if(True):
            #
            cur_day,cur_day_str,cur_day_trim_str,cur_minute_str = get_cur_day()
            #
            all_num,left_num,ret_left,stock_price_dict,all_stock_rows = get_easyquotation_stock_data(conf['data_source'])
            print (all_num,left_num,ret_left)
            if all_num:
                result = add_minute_data_to_redis(stock_price_dict,conf['redis_host'],conf['redis_port'],conf['redis_db']);
                print (tm,",add_minute_data_to_redis(), set redis:",result)
            else:
                print (tm,"easyquotation data get error!")
        else:
            print (tm,"market closed.")
        #break
        time.sleep(60)


# In[ ]:

run(config_parms)


# In[ ]:




# In[ ]:




# In[ ]:



