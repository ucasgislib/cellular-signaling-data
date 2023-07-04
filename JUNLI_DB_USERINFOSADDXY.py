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
        self.conn = psycopg2.connect(database="bjcellular", user="postgres", password="123456", host="127.0.0.1", port="5432")
        self.cur = self.conn.cursor()

    def read_mapper_output(self, file, separator=','):
        for line in file:
            yield [line.rstrip().split(separator)[0],line.rstrip().split(separator)]

    def create_table(self):
        tabName = "bj_userinfos_xy"
        seqName = tabName + '_id_seq'
        sql = """
            DROP SEQUENCE IF EXISTS %s CASCADE;
            CREATE SEQUENCE %s;

            DROP TABLE IF EXISTS %s;
            CREATE TABLE %s (
                imsi        bigint,
                home_lac        int,
                home_cid        int,
                work_lac        int,
                work_cid        int,
                traffic_type        int,
                file_num        int,
                home_community_id        int,
                home_district_id        int,
                work_community_id        int,
                work_district_id        int,
                home_lon        float,
                home_lat        float,
                work_lon        float,
                work_lat        float
            )
        """ % (seqName,seqName,tabName,tabName)
        self.cur.execute(sql)
        self.conn.commit()

    def run(self):
        print "create table..."
        self.create_table()
        exit()
        print "query table..."
        sql = "select * from bj_userinfos2;"
        self.cur.execute(sql)
        rec_li = self.cur.fetchall()
        print "start loop..."
        cp = 0
        for rec in rec_li:
            imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id = rec
            sql = "select * from bj_cellinfos t where lac=%d and cid = %d"%(home_lac,home_cid)
            self.cur.execute(sql)
            homexy = self.cur.fetchall()[0]
            home_lon,home_lat = homexy[7],homexy[8]

            sql = "select * from bj_cellinfos t where lac=%d and cid = %d"%(work_lac,work_cid)
            self.cur.execute(sql)
            workxy = self.cur.fetchall()[0]
            work_lon,work_lat = workxy[7],workxy[8]
            sql = '''INSERT INTO bj_userinfos_xy (imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id,home_lon,home_lat,work_lon,work_lat)
                VALUES (%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f);
             '''%(imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id,home_lon,home_lat,work_lon,work_lat)
            self.cur.execute(sql)
            
            cp += 1
            if cp % 1000 ==0:
                print cp
                self.conn.commit()


#######################################################
if __name__ == "__main__":
    bat = BATCH()
    bat.run()
    del bat