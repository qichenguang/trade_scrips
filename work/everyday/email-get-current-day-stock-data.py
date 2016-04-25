
# coding: utf-8

# In[100]:

#!/usr/bin/python


# email 
import poplib
import email
from email import parser
# http unzip
import urllib2
import sys, httplib
import os
import zipfile  
import datetime


# In[101]:

config_parms = {
    'email_username':'13701386551@163.com',
    'email_password':'qxy20130703',
    'email_pop3_host':'pop.163.com',
    'email_from':'yucezhe.com',
    'output_everyday_stock_index_data_path' : 'E:/project/pychram/traderesp/base/input-csv/everyday-index-stock/',
}


# In[102]:

class POP3Email:
    user = ''
    password = ''
    host=''
    M = None
    def __init__(self):
        pass
    
    def login(self,user,password,host='pop.163.com'):
        '''
            登录
        '''
        self.user = user
        self.password = password
        self.M = poplib.POP3(host)  
        self.M.user(self.user)    
        self.M.pass_(self.password)  
        
    def getEmailNum(self):
        #打印有多少封信    
        numMessages = len(self.M.list()[1])    
        #print 'num of messages', numMessages   
        return numMessages
    
    def getEmailbySubject(self,from_email,date_str,payloadpath):
        numMessages = self.getEmailNum()
        #print numMessages
        
        #从最老的邮件开始遍历  
        for i in range(numMessages):
            #print "i=",i
            m = self.M.retr(i+1)  
            msg = email.message_from_string('\n'.join(m[1]))  
            #allHeaders = email.Header.decode_header(msg)  
            aimHeaderStrs = {'from':'', 'to':'', 'subject':''}  
            for aimKey in aimHeaderStrs.keys():  
                aimHeaderList = email.Header.decode_header(msg[aimKey])  
                for tmpTuple in aimHeaderList:  
                    if tmpTuple[1] == None:  
                        aimHeaderStrs[aimKey] += tmpTuple[0]  
                    else:  
                        #转成unicode 
                        aimHeaderStrs[aimKey] += tmpTuple[0].decode(tmpTuple[1])  
            msg_from = aimHeaderStrs['from']
            msg_subject = aimHeaderStrs['subject']
            #print msg_from
            #print msg_subject
            if msg_from.find(from_email) != -1 and msg_subject.find(date_str) != -1:
                # subject
                for aimKey in aimHeaderStrs.keys():  
                    #转成utf-8显示
                    #print aimKey,':',aimHeaderStrs[aimKey].encode('utf-8')   
                    pass
                
                #遍历所有payload  
                for part in msg.walk(): 
                        contenttype = part.get_content_type()  
                        filename = part.get_filename()  
                        if filename: #and contenttype=='application/octet-stream':  
                                #保存附件  
                                #print filename
                                data = part.get_payload(decode=True)  
                                zipfilename = "%s%s" % (payloadpath,filename)
                                file(zipfilename,'wb').write(data) 
                                return zipfilename
                        elif contenttype == 'text/plain':  
                                #保存正文  
                                data = part.get_payload(decode=True)  
                                charset = part.get_content_charset('ios-8859-1')  
                                #file('mail%d.txt' % (i+1), 'w').write(data.decode(charset).encode('utf-8'))  
                                pass
            else:
                #print 'not stock email'
                continue
        return "No data. from email"


# In[103]:

def unzip_dir(zipfilename, unzipdirname):  
    fullzipfilename = os.path.abspath(zipfilename)  
    fullunzipdirname = os.path.abspath(unzipdirname)  
    print "Start to unzip file %s to folder %s ..." % (zipfilename, unzipdirname)  
    #Check input ...  
    if not os.path.exists(fullunzipdirname):  
        os.mkdir(fullunzipdirname)                
    #Start extract files ...  
    srcZip = zipfile.ZipFile(fullzipfilename, "r")  
    for eachfile in srcZip.namelist():  
        print "Unzip file %s ..." % eachfile  
        eachfilename = os.path.normpath(os.path.join(fullunzipdirname, eachfile))  
        eachdirname = os.path.dirname(eachfilename)  
        if not os.path.exists(eachdirname):  
            os.makedirs(eachdirname)  
        fd  = open(eachfilename, "wb")  
        fd.write(srcZip.read(eachfile))  
        fd.close()  
    srcZip.close()  
    print "Unzip file succeed!"  


# In[104]:

def get_email_data_and_unzip(username,password,pop3_host,email_from,date_str,out_path):
    #
    email_163 = POP3Email()
    email_163.login(username,password,pop3_host)
    zipfilename = email_163.getEmailbySubject(email_from,date_str,out_path)
    print zipfilename
    if zipfilename.find("No data.") == 0:
        print "Now no data from email"
    else:
        print "Now do unzip."
        outpath = zipfilename.split('.zip')[0]
        outpath = outpath[0:-4] + "-" + outpath[-4:-2] + "-" + outpath[-2:]
        unzip_dir(zipfilename,outpath)


# In[105]:

# 得到当前时间串
def get_cur_day():
    cur_day = datetime.datetime.now() 
    cur_day_str = cur_day.strftime('%Y-%m-%d'); 
    cur_day_trim_str = cur_day.strftime('%Y%m%d');
    cur_minute = cur_day.strftime('%Y-%m-%d %H:%M:%S'); 
    return cur_day,cur_day_str,cur_day_trim_str,cur_minute


# In[106]:

def run(conf):
    starttime = datetime.datetime.now()
    print "begin:",starttime
    cur_day,cur_day_str,cur_day_trim_str,cur_minute = get_cur_day()
    #############################################################################
    get_email_data_and_unzip(conf["email_username"],
                             conf["email_password"],
                             conf["email_pop3_host"],
                             conf["email_from"],
                             cur_day_trim_str,
                             conf["output_everyday_stock_index_data_path"])
    #############################################################################
    endtime = datetime.datetime.now()
    print "end:",endtime
    print "use(seconds):",str((endtime - starttime).seconds)
    print "#############################################################################"


# In[107]:

run(config_parms)


# In[ ]:




# In[ ]:



