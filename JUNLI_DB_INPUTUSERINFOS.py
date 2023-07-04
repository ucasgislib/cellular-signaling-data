# -*- coding:utf-8 -*-

import os
import sys 
import time
import math
import pg
import traceback
from datetime import datetime
from pyproj import Proj
import numpy as np

class DATA2DB:

    def __init__(self):
        #
        self.db = pg.DB(host='localhost',port=5432,dbname='bjcellular',user='postgres',passwd='123456')
        self.init_proj()

    def __del__(self):
        self.db.close()
    #
    def init_proj(self):
        params = {'proj':'merc','a':6378137,'b':6378137,'lat_ts':0,'lon_0':0,'x_0':0,'y_0':0,'k':1.0,'units':'m','nadgrids':'@null'}
        self.proj900913 = Proj(params)

    def read_files(self,rootdir):
        print 'read files ...'
        filelist = []
        # print rootdir
        for curdir, subfolders, files in os.walk(rootdir):
            for file in files:
                f = os.path.join(curdir,file)
                # print f,os.path.getsize(f)                     #os.path.getsize(f)返回文件大小 （字节为单位）
                #print 
                # fh = open(f)
                # lins = fh.readlines()

                filelist.append(f)

        return filelist

    def loop_files(self,filelist):

        for i in filelist:
            t0 = time.time()
            filenum = int(i[-3:])
            tabName = self.tablename+"_%03d"%filenum
            self.create_table(tabName)
            sql = "COPY %s FROM '%s'  WITH CSV  HEADER;"%(tabName,i)
            print sql
            self.db.query(sql)
            t1 = time.time()
            print filenum,t1-t0
            exit()

    def create_table(self,tabName):
        print "create table."
        # tabName = self.tablename
        seqName = tabName + '_id_seq'
        # [year_mon,tim,x,y,CID,CEVENT,CSTATE,GSPEED,GAZM,GSTATE]

        sql = """
            DROP SEQUENCE IF EXISTS %s CASCADE;
            CREATE SEQUENCE %s;

            DROP TABLE IF EXISTS %s;
            CREATE TABLE %s (
                imsi        bigint,
                h_lac      int,
                h_cid      int,
                w_lac      int,
                w_cid      int,
                traffic_type      int,
                file_num      int
            )
        """ % (seqName,seqName,tabName,tabName)
        self.db.query(sql)

    def run(self,rootdir):
        #
        self.tablename = "bj_userinfos_916w"
        
        filelist = self.read_files(rootdir)
        print len(filelist)

        self.loop_files(filelist)


if __name__ == '__main__':
    
    rootdir = r'D:\00_Work\2017YFB0503700\format_data'
    test = DATA2DB()  
    test.run(rootdir)   