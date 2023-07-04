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
            'password': 'ucas2019',
            'dbname': 'lbs',
            'port': 5432
        }

        return db_conn_config

    def cal_Eu_dist(self,x1,y1,x2,y2):
        x,y = x2-x1, y2-y1
        tda = math.sqrt(x**2+y**2)

        return tda

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
        SQL0 = "SELECT count(*) FROM bj_userinfos where traffic_type = 1104;"
        self.cur.execute(SQL0)
        print self.cur.fetchall()
        SQL0 = "SELECT imsi,home_cid,home_lac,work_cid,work_lac,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id FROM bj_userinfos where traffic_type = 1104;"
        self.cur.execute(SQL0)
        res = self.cur.fetchall()
        cp = 0
        cq=0
        t1=time.time()
        for num, row in enumerate(res):
            imsi,home_cid,home_lac,work_cid,work_lac,o_traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id = row
            try:
                home_sel = "SELECT x,y FROM bj_cellinfos where lac=%d and cid = %d"%(home_lac,home_cid)
                self.cur.execute(home_sel)
                home_x,home_y = self.cur.fetchone()
                work_sel = "SELECT x,y FROM bj_cellinfos where lac=%d and cid = %d"%(work_lac,work_cid)
                self.cur.execute(work_sel)
                work_x,work_y = self.cur.fetchone()
                dist = self.cal_Eu_dist(home_x,home_y,work_x,work_y)
                dist = dist/1000.00 
                tq1,tq2,tq3,tq4 = 0.98,3.9,17.4,42.5

                if 0<= float(dist) <=tq1:
                    traffic_type = 1101
                elif  tq1< float(dist) <= tq2:
                    traffic_type = 1102
                elif  tq2< float(dist) <= tq3:
                    traffic_type = 1103
                else:
                    traffic_type = 1104
                
                if traffic_type == o_traffic_type:
                    continue
                # print imsi,home_lac,home_cid,work_lac,work_cid,dist,traffic_type
                # SQL = '''DELETE FROM bj_userinfos_final WHERE imsi = %d;'''%(imsi)
                # print SQL
                SQL = '''UPDATE bj_userinfos SET traffic_type = %d WHERE imsi = %d ;'''%(traffic_type,imsi)
                self.cur.execute(SQL)
           #      # print "----------"
           #      SQL_INSERT = '''INSERT INTO bj_userinfos_final (imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id) 
           # VALUES(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d);'''%(imsi,home_lac,home_cid,work_lac,work_cid,traffic_type,file_num,home_community_id,home_district_id,work_community_id,work_district_id)
           #      # print SQL_INSERT
           #      self.cur.execute(SQL_INSERT)
                cq+=1
            except:
                pass
            cp+=1
            if cp % 100 ==0:
                t2=time.time()
                print cp,cq,t2-t1
                self.conn.commit()
        print cp
        self.conn.commit()

    def run(self):

        TableName = "bj_userinfos_final_new" 
        # TabFields = [('imsi','bigint'),('home_lac','int'),('home_cid','int'),('work_lac','int'),('work_cid','int'),('traffic_type','int'),('file_num','int')]
        #             ,('home_community_id','int'),('home_district_id','int'),('work_community_id','int'),('work_district_id','int')]

        # self.Create_Table(TableName,TabFields)

        self.sel_data(TableName)




if __name__ == '__main__':
    ud = UPDATE()
    ud.run()