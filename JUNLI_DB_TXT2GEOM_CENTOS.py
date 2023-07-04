#coding=utf-8

import calendar
import time,os,glob,string,datetime
import zipfile,tarfile
import traceback
import collections
import math,random
import threading
import numpy as np,pandas as pd
from pandas import Series,DataFrame
import os,sys,copy
import cPickle as pickle
import json
import  psycopg2
from datetime import datetime,timedelta
from itertools import groupby
from operator import itemgetter


######################################################################

class BATCH():
    def __init__(self):
        with open('./pkl/bj_cellinfo_20150102.pkl', 'rb') as f:
            self.cell_dict = pickle.load(f)
        self.conn = psycopg2.connect(database="lbs", user="postgres", host="127.0.0.1", port="5432")
        self.cur = self.conn.cursor()

    def read_mapper_output(self, file, separator=','):
        for line in file:
            yield [line.rstrip().split(separator)[0],line.rstrip().split(separator)]

    def create_table(self):
        tabName = "bj_data2015"
        seqName = tabName + '_id_seq'
        sql = """
            DROP SEQUENCE IF EXISTS %s CASCADE;
            CREATE SEQUENCE %s;

            DROP TABLE IF EXISTS %s;
            CREATE TABLE %s (
                imsi        bigint,
                geom      geometry
            )
        """ % (seqName,seqName,tabName,tabName)
        self.cur.execute(sql)
        self.conn.commit()

    def run(self):
        self.create_table()
        for i in range(1,500):
            print "===========  %s  =========="%i
            f=open("../format_data/format-00%03d"%i,'r')
            # fo = open("../format_data/format-00%03d"%i,'w')
            data = self.read_mapper_output(f, separator=',')
            cp,cq,co = 0,0,0
            t0 = time.time()
            for uid, group in groupby(data, itemgetter(0)):

                wkt_line = ""
                xy_li = []
                for li in list(group):
                    try:                    
                        imsi,tim,lac,cid,lon,lat = li[1]
                        timestamp = time.mktime(time.strptime(tim, "%Y-%m-%d %H:%M:%S")) 
                        xy_li.append([str(lon),str(lat),int(timestamp)])
                    except:
                        pass

                for xy in xy_li:
                    wkt_line = wkt_line + "%s %s %d,"%(xy[0],xy[1],xy[2])
                if len(xy_li) == 1:
                    wkt_line = 'POINT(%s)'%wkt_line[:-1]
                else:
                    wkt_line = 'LINESTRING(%s)'%wkt_line[:-1]
                # if len(wkt_line) >10:
                sql = '''
                    INSERT INTO bj_data2015 (imsi,geom) VALUES ( %d,ST_GeomFromText('%s',3857));
                '''%(int(uid),wkt_line)
                # print sql
                self.cur.execute(sql)
                
                cp += 1
                t1=time.time()
                if cp%10000 == 0: 
                    print cp,cq,co,t1-t0
                cq+=1
            self.conn.commit()
            f.close()
            # exit()


#######################################################
if __name__ == "__main__":
    bat = BATCH()
    bat.run()
    del bat