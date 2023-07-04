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
            print u'SQL执行失败，执行语句为:%s' % str(sql)

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

    TableName = "bj_cellinfos_xyid" 
    TabFields = [('lac','int'),('cid','int'),('lon','float'),('lat','float'),('x','float'),('y','float'),('qh_name','text'),('district_name','text'),('community_id','int'),('district_id','int')]
    pUtils = PostgressUtils()
    pUtils.Create_Table(TableName,TabFields)

    logFile = './data_input_orig2.log'

    ##########定义三个游标########################
    cur0 = pUtils.conn.cursor()
    cur1 = pUtils.conn.cursor()
    cur2 = pUtils.conn.cursor()
    SQL0 = "SELECT COUNT (1) FROM bj_cellinfos t1"
    SQL1 = """
            SELECT
            t1.lac,
            t1.cid,
            t1.lon,
            t1.lat,
            t1.x,
            t1.y
        FROM
           bj_cellinfos t1
        """

    SQL2 = "SELECT t.area_id, t.district_id, st_astext(t.geom) FROM bj_community t"
    SQL3 = "INSERT INTO bj_cellinfos_id(lac,cid,lon,lat,x,y,community_id,district_id)VALUES {0}"
    
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
    fw = open("cellinfos.csv",'w')
    for num, row in enumerate(cur1):
        lac,cid,lon,lat,x,y = row
        if x == 0 and y ==0 and lon > 0 and lat>0:
            x,y = pUtils.proj900913(lat,lon)

        home_wkt = 'POINT(%f %f)'%(x,y)
        g = ogr.CreateGeometryFromWkt(home_wkt)
        hareaId, hdistrictId = get_area(g, cuomunityLis)
        hareaId, hdistrictId = int(hareaId), int(hdistrictId)

        try:
            SQL = "SELECT t.qh_name, t.district_name FROM bj_community t where t.area_id = %d  and t.district_id = %d "%( hareaId, hdistrictId)
            pUtils.cur.execute(SQL)
            qh_name,district_name = pUtils.cur.fetchone()
        except:
            qh_name,district_name = '北京','北京'
        rec  = "%d,%d,%f,%f,%f,%f,'%s','%s',%d,%d"%((lac),(cid),(lon),(lat),(x),(y),qh_name,district_name,hareaId, hdistrictId)
        fw.write(rec+"\n")
        SQL3 = "INSERT INTO bj_cellinfos_xyid (lac,cid,lon,lat,x,y,qh_name,district_name,community_id,district_id)VALUES (%d,%d,%f,%f,%f,%f,'%s','%s',%d,%d)"%((lac),(cid),(lon),(lat),(x),(y),qh_name,district_name,hareaId, hdistrictId)
        print SQL3
        pUtils.cur.execute(SQL3)
        pUtils.conn.commit()
        # print (str(imsi), home_cid, home_lac, work_cid, work_lac, traffic_type, file_num, hareaId, hdistrictId, wareaId, wdistrictId)
        # updataList.append((str(lac),str(cid),str(lon),str(lat),str(x),str(y),hareaId, hdistrictId))
        # SQL3_1 = str(updataList)[1:][:-1]
        # SQL3_T = SQL3.format(SQL3_1)
        # pUtils.executeSQLVoidCommit1(SQL3_T)
        # pUtils.executeCommit1()
        # updataList = []
        # if (num % 1000 == 0) or (num == (totalNum - 1)):
        #     SQL3_1 = str(updataList)[1:][:-1]
        #     SQL3_T = SQL3.format(SQL3_1)
        #     pUtils.executeSQLVoidCommit1(SQL3_T)
        #     pUtils.executeCommit1()
        #     updataList = []
        #     t2 = time.time()
        #     print num, u'处理', NUM, u'条用时', t2 - t1, u'秒!'

    cur1.close()
    del pUtils
