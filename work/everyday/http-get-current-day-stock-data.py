
# coding: utf-8

# In[ ]:

#!/usr/bin/python


import urllib2
import sys, httplib
import os
import zipfile  
#%pwd


# In[ ]:

config_parms = {
    'output_everyday_stock_index_data_path' : 'E:/project/pychram/traderesp/base/input-csv/everyday-index-stock/',
    'everyday_stock_index_data_url'   : 'http://yucezhe.com/api/download/product/latest?token=2556e43c18739&pdt_name=overview-push'
}


# In[ ]:

def down_file(url,outpath):
    #
    file_name = url.split('/')[-1]
    file_name = file_name.split('?')[0]
    print file_name
    file_name = outpath + file_name
    unzip_file_path = file_name.split('.zip')[0]
    print file_name
    print unzip_file_path
    #
    u = urllib2.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    
    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break
        #
        file_size_dl += len(buffer)
        f.write(buffer)
    f.close()
    
    return file_name,unzip_file_path


# In[ ]:

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


# In[ ]:

def get_url_data_and_unzip(url,out_path):
    f = urllib2.urlopen(url)
    cont = f.read()
    print cont
    if cont.find("No data.") == 0:
        print "Now no data."
    else:
        print "Now do download."
        file_name,unzip_file_path = down_file(cont,out_path)
        unzip_dir(file_name,unzip_file_path)


# In[ ]:

def run(conf):
    get_url_data_and_unzip(conf["everyday_stock_index_data_url"],conf["output_everyday_stock_index_data_path"])


# In[ ]:

run(config_parms)


# In[ ]:



