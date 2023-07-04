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
from osgeo import gdal, osr, ogr
from osgeo.gdalconst import *

reload(sys)
sys.setdefaultencoding('utf8')

NUM = 100000

class PostgressUtils(object):
    ##################################################################
    ##########自行管理不需要手动管理######################################
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

    def executeSQLfetchAll1(self, sql):
        try:
            self.cur.execute(sql)
            res = self.cur.fetchall()
            return res
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    def executeSQLfetchOne1(self, sql):
        try:
            self.cur.execute(sql)
            res = self.cur.fetchone()
            return res
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    def executeSQLVoidCommit1(self, sql):

        try:
            self.cur.execute(sql)
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    def executeCommit1(self):
        try:
            self.conn.commit()
        except:
            print 'SQL执行失败，执行语句为:%s'

    ##########手动管理不需要手动管理######################################
    #通过一个连接获取不同的游标
    def execute2(self, cur,sql):
        try:
            cur.execute(sql)
            return cur
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    def executeSQLfetchAll2(self, cur,sql):
        try:
            cur.execute(sql)
            res = cur.fetchall()
            return res
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    def executeSQLfetchOne2(self, cur,sql):
        try:
            cur.execute(sql)
            res = cur.fetchone()
            return res
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    # 通过一个连接获取不同的游标
    def executeSQLVoidCommit2(self,cur,sql):

        try:
            cur.execute(sql)
        except:
            print 'SQL执行失败，执行语句为:%s' % str(sql)

    # 通过一个连接获取不同的游标
    def executeCommit2(self):
        try:
            self.conn.commit()
        except:
            print 'SQL执行失败，执行语句为:%s'


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

# 将内容写入到指定文件
def writeFile(targetFile, content):
    fd = open(targetFile, 'a+')
    fd.write(content)


# 将日志写入到指定文件中
def registLog(logFile, msg):
    writeFile(logFile, str(msg) + "\n")
    print str(msg)


def get_area(g, communitylis):
    areaId = 0
    districtId = 0
    for com in communitylis:
        area_id, districtId, geom = com
        isIntersect = geom.Intersect(g)
        # print isIntersect
        if isIntersect:
            areaId = area_id
            districtId = districtId
            return areaId, districtId
    return areaId, districtId


if __name__ == '__main__':

    TableName = "bj_userinfos_final1" 
    TabFields = [('imsi','bigint'),('home_lac','int'),('home_cid','int'),('work_lac','int'),('work_cid','int'),('traffic_type','int'),('file_num','int')
                ,('home_community_id','int'),('home_district_id','int'),('work_community_id','int'),('work_district_id','int')]
    pUtils = PostgressUtils()
    pUtils.Create_Table(TableName,TabFields)

    logFile = './data_input_orig2.log'

    ##########定义三个游标########################
    cur0 = pUtils.conn.cursor()
    cur1 = pUtils.conn.cursor()
    cur2 = pUtils.conn.cursor()
    SQL0 = "SELECT COUNT (1) FROM bj_userinfos_final_noid t1"
    SQL1 = """
            SELECT
            t1.imsi,
            t1.home_cid,
            t1.home_lac,
            t1.work_cid,
            t1.work_lac,
            t1.traffic_type,
            t1.file_num,
            t2.lon as home_lon,
            t2.lat as home_lat,
            t2.lon as work_lon,
            t3.lat as work_lat
        FROM
           bj_userinfos_final_noid t1
        JOIN bj_cellinfos t2 ON t1.home_cid = t2.cid AND t1.home_lac = t2.lac
        JOIN bj_cellinfos t3 ON t1.work_cid = t3.cid AND t1.work_lac = t3.lac
        
        """
    SQL2 = "SELECT t.area_id, t.district_id, st_astext(t.geom) FROM bj_community t"
    SQL3 = "INSERT INTO bj_userinfos_final(imsi,home_cid,home_lac,work_cid,work_lac,traffic_type," \
           "file_num,home_community_id,home_district_id,work_community_id,work_district_id)VALUES {0}"
    
    # 获取记录条数
    totalNum = pUtils.executeSQLfetchOne2(cur0, SQL0)[0]
    # 获取街区的表
    cuomunityLis = pUtils.executeSQLfetchAll2(cur2, SQL2)
    cuomunityLis = [(cum[0], cum[1], ogr.CreateGeometryFromWkt(cum[2])) for cum in cuomunityLis]
    cur0.close()
    cur2.close()
    # 将数据进行更新插入
    updataList = []
    print "update....."
    cur1 = pUtils.execute2(cur1, SQL1)
    print "update.....query done"
    t1 = time.time()
    for num, row in enumerate(cur1):
        imsi, home_cid, home_lac, work_cid, work_lac, traffic_type, file_num, home_lon,home_lat, work_lon,work_lat = row
        home_wkt = 'POINT(%f %f)'%(pUtils.proj900913(home_lon,home_lat))
        g = ogr.CreateGeometryFromWkt(home_wkt)
        hareaId, hdistrictId = get_area(g, cuomunityLis)
        hareaId, hdistrictId = int(hareaId), int(hdistrictId)
        work_wkt = 'POINT(%f %f)'%(pUtils.proj900913(work_lon,work_lat))
        g = ogr.CreateGeometryFromWkt(work_wkt)
        wareaId, wdistrictId = get_area(g, cuomunityLis)
        wareaId, wdistrictId = int(wareaId), int(wdistrictId)
        # wareaId, wdistrictId = 0, 0
        
        # print (str(imsi), home_cid, home_lac, work_cid, work_lac, traffic_type, file_num, hareaId, hdistrictId, wareaId, wdistrictId)
        updataList.append((str(imsi), home_cid, home_lac, work_cid, work_lac, traffic_type, file_num, hareaId, hdistrictId, wareaId, wdistrictId))
        if (num % 100000 == 0) or (num == (totalNum - 1)):
            SQL3_1 = str(updataList)[1:][:-1]
            SQL3_T = SQL3.format(SQL3_1)
            pUtils.executeSQLVoidCommit1(SQL3_T)
            pUtils.executeCommit1()
            updataList = []
            t2 = time.time()
            print num,  t2 - t1

    cur1.close()
    del pUtils
