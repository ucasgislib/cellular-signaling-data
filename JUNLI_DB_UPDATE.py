# -*- coding:utf-8 -*-
import os
import sys
import time
import math
import psycopg2
import traceback
from datetime import datetime
from pyproj import Proj
import numpy as np

class UPDATE():

    def __init__(self):
        params = self.config()
        self.conn = psycopg2.connect(**params)
        self.cur = self.conn.cursor()
        projparams = {'proj':'merc','a':6378137,'b':6378137,'lat_ts':0,'lon_0':0,'x_0':0,'y_0':0,'k':1.0,'units':'m','nadgrids':'@null'}
        self.proj900913 = Proj(projparams)

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def config(self):
        db_conn_config = {
            'host': 'localhost',
            'user': 'postgres',
            'password': '123456',
            'dbname': 'bjcellular',
            'port': 5432
        }

        return db_conn_config

    def Create_Table(self,TableName,Fields):
        sql = """
            DROP TABLE IF EXISTS %s;
            CREATE TABLE %s()
            """ % (TableName,TableName)
        self.cur.execute(sql)
        for field in Fields:
            f,dtype = field
            sql = """
                ALTER TABLE %s ADD COLUMN %s %s;
                """ % (TableName,f,dtype)
            self.cur.execute(sql)
        print 'table %s created!' %(TableName)

    def sel_data(self,TableName):
        SQL0 = "SELECT imsi,home_cid,home_lac,work_cid,work_lac,traffic_type,file_num FROM bj_userinfos_final_new;"
        self.cur.execute(SQL0)
        res = self.cur.fetchall()
        cp = 0
        t1=time.time()
        for num, row in enumerate(res):
            home_district_id,home_community_id,work_district_id,work_community_id = 0,0,0,0
            imsi, home_cid, home_lac, work_cid, work_lac, traffic_type, file_num = row
            home_sel = "SELECT district_id,community_id FROM bj_cellinfos_id where lac=%d and cid = %d"%(home_lac,home_cid)
            self.cur.execute(home_sel)
            home_res = self.cur.fetchall()
            work_sel = "SELECT district_id,community_id FROM bj_cellinfos_id where lac=%d and cid = %d"%(work_lac,work_cid)
            self.cur.execute(work_sel)
            work_res = self.cur.fetchall()
            if len(home_res)>0:
                home_district_id,home_community_id = home_res[0]
            if len(work_res)>0:
                work_district_id,work_community_id = work_res[0]
            # print home_community_id,home_district_id,work_community_id,work_district_id
            # print work_district_id,work_community_id
            SQL = '''INSERT INTO %s (imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id) 
           VALUES(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d) ;'''%(TableName,imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id)
            self.cur.execute(SQL)
            cp+=1
            if cp % 50000 ==0:
                t2=time.time()
                print cp,t2-t1
                self.conn.commit()
        print cp
        self.conn.commit()
    def run(self):

        TableName = "bj_userinfos_final" 
        TabFields = [('imsi','bigint'),('home_lac','int'),('home_cid','int'),('work_lac','int'),('work_cid','int'),('traffic_type','int'),('file_num','int')
                    ,('home_community_id','int'),('home_district_id','int'),('work_community_id','int'),('work_district_id','int')]

        self.Create_Table(TableName,TabFields)

        self.sel_data(TableName)




if __name__ == '__main__':
    ud = UPDATE()
    ud.run()