
# coding: utf-8

# In[ ]:

from __future__ import division
from numpy.random import randn
from pandas import Series
import numpy as np
import pandas as pd
import os
import datetime  
import time
np.set_printoptions(precision=4)
import sys; 
import MySQLdb;


# In[ ]:

config_parms = {
    #每日的指数 CSV 数据文件目录(20151218之后的文件)
    'input_everyday_index_data_path' : 'E:/project/pychram/traderesp/base/input-csv/everyday-index-stock/',
    'output_index_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-index-m26/today_index_m26.txt',
    'output_stock_file_name' : 'E:/project/pychram/traderesp/base/input-csv/end-is-today-stock-ema12-ema26-macd/today_stock_macd.txt',
    'output_log_file_path'   : 'E:/project/pychram/traderesp/base/output-csv/log/',
    'output_log_type'        : 101,
    'mysql_host'             : '127.0.0.1',
    'mysql_port'             : 3306,
    'mysql_user'             : 'root',
    'mysql_passwd'           : 'root',
    'mysql_db_name'          : 'at'
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

# 得到 last http result
def get_last_http_result(everyday_path):
    # ========== 遍历数据文件夹中所有目录名
    dir_list = []
    for root, dirs, files in os.walk(everyday_path):# 注意：这里请填写电脑中的路径
        if dirs:
            for dir in dirs:
                if 'overview-push-' in dir:
                    dir_list.append(dir.split('overview-push-')[1])

    #print dir_list
    cur_date_series = pd.Series(dir_list)
    cur_date_dataframe = pd.DataFrame(cur_date_series)
    cur_date_dataframe.columns=['date']
    last_day = cur_date_dataframe.max()['date']
    return last_day
    


# In[ ]:

# 得到 last index result
def get_last_index_result(index_m26_file_name):
    handle = open(index_m26_file_name,"r")
    lines = handle.readlines();
    lines = list(reversed(lines))
    return lines[0].strip()


# In[ ]:

# 得到 last stock result
def get_last_stock_result(stock_macd_file_name):
    handle = open(stock_macd_file_name,"r")
    lines = handle.readlines();
    lines = list(reversed(lines))
    return lines[0].strip()


# In[ ]:

# 将结果写入文件
def write_log_to_file(output_log_file_path,output_log_type,msg):
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    log_file = output_log_file_path + cur_day_str + ".log";
    with open(log_file,'a') as handle:
        handle.writelines(str(output_log_type) + ":" + str(msg) + "\n")
        handle.close()


# In[ ]:

#将结果写入数据库
# 打开数据库连接
def write_log_to_db(mysql_host,mysql_port,mysql_user,mysql_passwd,mysql_db_name,output_log_type,log_msg):
    db = MySQLdb.connect(mysql_host,mysql_user,mysql_passwd,mysql_db_name )
    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()
    # SQL 插入语句
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    sql = "INSERT INTO AT_LOG(LX,LOG_TIME,LOG_MSG) VALUES ('%d', '%s', '%s' )" % (output_log_type, cur_minute, str(log_msg))
    print sql
    try:
       # 执行sql语句
       cursor.execute(sql)
       # 提交到数据库执行
       db.commit()
    except:
       # Rollback in case there is any error
       db.rollback()
    # 关闭数据库连接
    db.close()


# In[ ]:

#传入 String 得到 datetime 
def get_datetime_from_str(input_str):
    time_day = time.strptime(input_str, '%Y-%m-%d')
    dt_day = datetime.datetime(*time_day[:6])
    return dt_day


# In[ ]:

def run(conf):
    starttime = datetime.datetime.now()
    print "begin:",starttime    
    #############################################################################    
    dt_today = datetime.datetime.today()
    log_msg = '<font color="red">MAYBE HAVE SOME ERROR,PLEASE CHECK</font>'
    if(dt_today.hour < 19):
        log_msg = '<font color="red">Please run after 20 hours every evening</font>'
    else:
        last_http_day_str = get_last_http_result(conf['input_everyday_index_data_path'])
        last_index_day_str = get_last_index_result(conf['output_index_file_name'])
        last_stock_day_str = get_last_stock_result(conf['output_stock_file_name'])

        last_http_day = last_http_day_str.split(',')[0]
        last_index_day = last_index_day_str.split(',')[0]
        last_stock_day = last_stock_day_str.split(',')[0].split(' ')[0]
        print 'http     last day is ' + last_http_day
        print 'index    last day is '  + last_index_day 
        print 'stock    last day is '  + last_stock_day 

        dt_http_day  = get_datetime_from_str(last_http_day)
        dt_index_day = get_datetime_from_str(last_index_day)
        dt_stock_day = get_datetime_from_str(last_stock_day)
        #
        interval_http = (dt_today - dt_http_day).days
        interval_index = (dt_today - dt_index_day).days
        interval_stock = (dt_today - dt_stock_day).days
        print interval_http,interval_index,interval_stock
        week_day = dt_today.weekday()
        print week_day

        if(interval_http == 0 and interval_index == 0 and interval_stock == 0):
            log_msg = "OK.  NOW IS WORKDAY"
        elif (week_day == 5 or week_day == 6):
            if(week_day == 5 and interval_http == 1 and interval_index == 1 and interval_stock == 1):   
                log_msg = "OK.  NOW IS SATURDAY"
            if(week_day == 6 and interval_http == 2 and interval_index == 2 and interval_stock == 2):   
                log_msg = "OK.  NOW IS SUNDAY"   
        else:
            log_msg = '<font color="red">MAYBE HAVE SOME ERROR,PLEASE CHECK</font>'

        print dt_http_day,dt_index_day,dt_stock_day,dt_today
    write_log_to_file(conf['output_log_file_path'],conf['output_log_type'],log_msg)
    write_log_to_db(conf['mysql_host'],conf['mysql_port'],conf['mysql_user'],conf['mysql_passwd'],conf['mysql_db_name'],conf['output_log_type'],log_msg)
    #############################################################################
    endtime = datetime.datetime.now()
    print "end:",endtime
    print "use(seconds):",str((endtime - starttime).seconds)     
    print "#############################################################################"


# In[ ]:

run(config_parms)


# In[ ]:



