
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
import httplib
import traceback
import urllib
import StringIO, gzip


# In[ ]:

config_parms = {
    'token' : '690111381bf285eb0d678863033ab682105aee2bcd2960eefbc9eeb1a69915b7',
    'rts_save_file_path' : 'thefile.csv',
    'redis_host' : 'localhost',
    'redis_port' : 6379,
    'redis_db'   : 0
}


# In[ ]:

HTTP_OK = 200
HTTP_AUTHORIZATION_ERROR = 401
class Client:
    domain = 'api.wmcloud.com'
    port = 443
    token = ''
    httpClient = None
    def __init__( self ):
        self.httpClient = httplib.HTTPSConnection(self.domain, self.port)
    def __del__( self ):
        if self.httpClient is not None:
            self.httpClient.close()
    def encodepath(self, path):
        #转换参数的编码
        start=0
        n=len(path)
        re=''
        i=path.find('=',start)
        while i!=-1 :
            re+=path[start:i+1]
            start=i+1
            i=path.find('&',start)
            if(i>=0):
                for j in range(start,i):
                    if(path[j]>'~'):
                        re+=urllib.quote(path[j])
                    else:
                        re+=path[j]  
                re+='&'
                start=i+1
            else:
                for j in range(start,n):
                    if(path[j]>'~'):
                        re+=urllib.quote(path[j])
                    else:
                        re+=path[j]  
                start=n
            i=path.find('=',start)
        return re
    def init(self, token):
        self.token=token
    def getData(self, path):
        result = None
        path='/data/v1'+path
        path=self.encodepath(path)
        # print path
        try:
            #set http header here
            self.httpClient.request('GET', path, headers = {"Authorization": "Bearer " + self.token,
                                                            "Accept-Encoding": "gzip, deflate"})
            #make request
            response = self.httpClient.getresponse()
            #read result
            if response.status == HTTP_OK:
                #parse json into python primitive object
                result = response.read()
            else:
                result = response.read()
            compressedstream = StringIO.StringIO(result)  
            gziper = gzip.GzipFile(fileobj=compressedstream)    
            result = gziper.read()
            if(path.find('.csv?')==-1):
                result=result.decode('utf-8').encode('GB18030')
            return response.status, result
        except Exception, e:
            #traceback.print_exc()
            raise e
        return -1, result


# In[ ]:

def getTickRTSnapshot(token,securityID='',assetClass='E',exchangeCD='XSHG,XSHE',field='lastPrice,shortNM',getType='csv',outfile='thefile.csv'):
    try:
        client = Client()
        client.init(token)
        if getType == 'csv':
            url2='/api/market/getTickRTSnapshot.csv?securityID=%s&assetClass=%s&exchangeCD=%s&field=%s'             % (securityID,assetClass,exchangeCD,field)
            #print url2
            code, result = client.getData(url2)
            if(code==200):
                file_object = open(outfile, 'w')
                file_object.write(result)
                file_object.close( )
                return True
            else:
                print code
                print result
                return False
        else:
            url1='/api/market/getTickRTSnapshot.json?securityID=&assetClass=E&exchangeCD=XSHG,XSHE&field=lastPrice,shortNM'
            code, result = client.getData(url1)
            if code==200:
                print result
                return True
            else:
                print code
                print result
                return False
    except Exception, e:
        traceback.print_exc()
        #raise e
        return False


# In[ ]:

def load_RtsSnapshot_and_filter_invalid_stock(inputfile='thefile.csv'):
    # 从csv文件中读取该股票数据 
    # 注意：这里请填写数据文件在您电脑中的路径
    stock_data = pd.read_csv(inputfile,dtype = {u'ticker': str},encoding='gbk')
    # print stock_data.columns
    # 选取需要的字段，去除其他不需要的字段
    #"timestamp","ticker","exchangeCD","shortNM","lastPrice"
    stock_data = stock_data[[ u'ticker',u'exchangeCD', u'shortNM',u'lastPrice']]
    stock_data.columns = ['code','market','shortname','price']
    
    #
    stock_price_dict = {}
    all_num = 0
    left_num = 0
    for id,row in stock_data.iterrows():
        all_num += 1
        code = str(row['code'])
        if len(code) == 6 and (int(code[0]) == 6 or (code[0]) == "0" or int(code[0]) == 3):
            pass
        else:
            #print "code error:", row['code'],row['shortname']
            continue 
        # 过滤其他市场
        if  row['market'] != 'XSHG' and row['market'] != 'XSHE':
            #print "market error:", row['code'],row['shortname']
            continue
        if int(row['code'][0]) == 6 and row['market'] != 'XSHG':
            #print "market 6 XSHG error:", row['code'],row['shortname']
            continue
        if (int(row['code'][0]) == 0 or  int(row['code'][0]) == 3 )and row['market'] != 'XSHE':
            #print "market 0 3 XSHE error:", row['code'],row['shortname']
            continue
        if row['shortname'].encode('UTF-8','ignore').find('指数') != -1:
            #print "指数 error:", row['code'],row['shortname']
            continue
        if row['shortname'].encode('UTF-8','ignore').find('上证') != -1:
            #print "上证 error:", row['code'],row['shortname']
            continue 
        if row['shortname'].encode('UTF-8','ignore').find('深证') != -1:
            #print "深证 error:", row['code'],row['shortname']
            continue
        if row['shortname'].encode('UTF-8','ignore').find('成指') != -1:
            #print "成指 error:", row['code'],row['shortname']
            continue
        if row['shortname'].encode('UTF-8','ignore').find('综指') != -1:
            #print "综指 error:", row['code'],row['shortname']
            continue        
        # 大于 1000 为指数
        if int(row['price']) > 1000:
            #print " > 1000 error:", row['code'],row['shortname']
            continue    
        # 停牌
        if float(row['price']) < 0.1:
            #print " < 0.1 error:", row['code'],row['shortname']
            continue            

        #print row
        left_num += 1
        stock_price_dict[row['code']] = int(row['price'] * 10000.0)
    return all_num,left_num,stock_price_dict


# In[ ]:

# 得到当前时间串
def get_cur_day():
    cur_day = datetime.datetime.now() 
    cur_day_str = cur_day.strftime('%Y-%m-%d'); 
    cur_day_trim_str = cur_day.strftime('%Y%m%d');
    cur_minute = cur_day.strftime('%Y-%m-%d %H:%M:%S'); 
    return cur_day,cur_day_str,cur_day_trim_str,cur_minute


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
        abeg = datetime.datetime(tm.year,tm.month,tm.day,9,30,0)
        aend = datetime.datetime(tm.year,tm.month,tm.day,11,30,0)
        bbeg = datetime.datetime(tm.year,tm.month,tm.day,13,0,0)
        bend = datetime.datetime(tm.year,tm.month,tm.day,15,0,0)
        if (tm > abeg and tm < aend) or (tm > bbeg and tm < bend) :
            get_ok = getTickRTSnapshot(conf['token'],outfile=conf['rts_save_file_path']);
            if get_ok:
                all_num,left_num,stock_price_dict = load_RtsSnapshot_and_filter_invalid_stock(inputfile=conf['rts_save_file_path'])
                result = add_minute_data_to_redis(stock_price_dict,conf['redis_host'],conf['redis_port'],conf['redis_db']);
                print tm,",add_minute_data_to_redis(), set redis:",result
            else:
                print tm,"RTS data get error!"
        else:
            print tm,"market closed."
        time.sleep(60)


# In[ ]:

run(config_parms)


# In[ ]:




# In[ ]:




# In[ ]:



