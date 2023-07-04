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
import  pg
from datetime import datetime,timedelta
from itertools import groupby
from operator import itemgetter
from pyproj import Proj
from geopy.distance import great_circle
from shapely.geometry  import MultiPoint,Polygon
from geopy.geocoders  import Nominatim

######################################################################

class BATCH():
    def __init__(self):
        with open('./pkl/bj_cellinfo_20150102.pkl', 'rb') as f:
            self.cell_dict = pickle.load(f)
        params = {'proj':'merc','a':6378137,'b':6378137,'lat_ts':0,'lon_0':0,'x_0':0,'y_0':0,'k':1.0,'units':'m','nadgrids':'@null'}
        self.proj900913 = Proj(params)
        self.db = pg.DB(host='localhost',port=5432,user='postgres',dbname='bjcellular',passwd='123456')


    def read_mapper_output(self, file, separator='\x01'):
        for line in file:
            yield [line.rstrip().split(separator)[4],line.rstrip().split(separator)]

    def myfindidx(self,x,y):
        return [ a for a in range(len(y)) if y[a] == x]


    def cal_Eu_dist(self,x1,y1,x2,y2):
        x,y = x2-x1, y2-y1
        tda = math.sqrt(x**2+y**2)
        # tda = x**2+y**2
        
        return tda

    def get_centermost_point(self,cluster):
        # print cluster
        cluster = [[float(c[1]),float(c[0])] for c in cluster]
        # print cluster
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        # print great_circle(point, centroid).m
        centermost_point = min(cluster, key=lambda point: great_circle(point, centroid).m)
        
        return tuple(centermost_point)

    def cal_rog(self,usr_tslist):
        ts_len = len(usr_tslist)

        xy = self.get_centermost_point(usr_tslist)
        cx,cy = self.proj900913(float(xy[1]),float(xy[0]))
        ts_dist = 0
        for rec in usr_tslist:
            try:
                lon,lat = rec
                x,y = self.proj900913(lon,lat)
                dis = self.cal_Eu_dist(x,y,cx,cy)
                ts_dist+=(dis/1000.00)
            except:
                pass
        if ts_len !=0:
            rog = math.sqrt(ts_dist/ts_len)
        else:
            rog = 0

        return rog

    def work_judge(self,m,h_lac,h_cid,w_lac,w_cid,u_typ,rog,home_cell):
        p40,p60,p90 = 1,3,6
        try:
            key_li,values_li = m.keys(),m.values()
            val_max = max(values_li)
            idx = random.choice(self.myfindidx(val_max,values_li))

            cell_max = key_li[idx]
            w_lac,w_cid = cell_max
        except:
            w_lac,w_cid = 0,0
        
        if h_lac ==0:
            try:
                home_counter = collections.Counter(home_cell)
                home_key_li,home_values_li = home_counter.keys(),home_counter.values()
                home_val_max = max(home_values_li)
                home_idx = random.choice(self.myfindidx(home_val_max,home_values_li))
                cell_max = home_key_li[idx]
                h_lac,h_cid = home_cell_max
            except:
                pass

        
        if h_lac == 0 and w_lac == 0:
            return 0,0,0,0,0000
            
        if h_lac == 0 and w_lac != 0:
            h_lac,h_cid =  w_lac,w_cid
            if  float(rog) <=p40:
                u_typ = 1001
            elif  p40< float(rog) <= p60:
                u_typ = 1002
            elif  p60< float(rog) <= p90:
                u_typ = 1003
            else:
                u_typ = 1004

        if h_cid == w_cid or w_cid == 0:
            if  float(rog) <=p40:
                u_typ = 1001
            elif  p40< float(rog) <= p60:
                u_typ = 1002
            elif  p60< float(rog) <= p90:
                u_typ = 1003
            else:
                u_typ = 1004
        else:
            if 0< float(rog) <=p40:
                u_typ = 1101
            elif  p40< float(rog) <= p60:
                u_typ = 1102
            elif  p60< float(rog) <= p90:
                u_typ = 1103
            else:
                u_typ = 1104

        return h_lac,h_cid,w_lac,w_cid,u_typ

    def run(self,tmin,tmax):
        #552 650
        work_hour = [9,10,11,14,15,16]
        home_hour = [23,24,0,1,2,3]
        fw = open("imsi_samehw%02d%02d.csv"%(tmin,tmax),'w')
        rog_li = []
        for i in range(tmin,tmax):
            print "===========  %s  =========="%i
            f=open("../bj02_imsitim/part-00%03d"%i,'r')
            # fo = open("../format_data/format-00%03d"%i,'w')
            data = self.read_mapper_output(f, separator='\x01')
            cp,cq,co = 0,0,0
            t0 = time.time()
            staytim_li,staysum_li = [],[]
            for uid, group in groupby(data, itemgetter(0)):
                #从数据库里查询家的位置
                query_sql = "select * from bj_userinfos  where imsi = %s and home_cid = work_cid"%uid
                que_res = self.db.query(query_sql).dictresult()
                if len(que_res) == 1:
                    data_imsi = que_res[0]
                    h_lac,h_cid,w_lac,w_cid,u_typ = data_imsi['home_lac'],data_imsi['home_cid'],data_imsi['work_lac'],data_imsi['work_cid'],data_imsi['traffic_type']
                    # print "------------------- %s -------------------"%uid
                    usr_tsdict = {}
                    usr_tsdict["uid"]=uid
                    cn = 0
                    for li in list(group):
                        try:
                            tim,lac,cid = datetime.utcfromtimestamp(int(li[1][2])),int(li[1][0]),int(li[1][1])
                            x1,y1 = self.cell_dict[(lac,cid)]["lon"],self.cell_dict[(lac,cid)]["lat"]
                            # orig_rec = ",".join([str(uid),str(tim),str(lac),str(cid),str(x1),str(y1)])+"\n"
                            # fo.write(orig_rec)
                            if cn ==0:
                                usr_tsdict["cell"]=[(lac,cid)]
                                usr_tsdict["lon"]=[x1]
                                usr_tsdict["lat"]=[y1]
                                usr_tsdict["tim"]=[tim]
                            else:
                                usr_tsdict["cell"].append((lac,cid))
                                usr_tsdict["lon"].append(x1)
                                usr_tsdict["lat"].append(y1)
                                usr_tsdict["tim"].append(tim)
                        except:
                            pass
                        cn+=1
                    usr_tslist,work_cell,home_cell = [],[],[]
                    
                    if 'tim' in usr_tsdict.keys() and len(usr_tsdict["tim"])>5:
                        df = DataFrame(usr_tsdict,columns=["cell","lon","lat","tim"])
                        X = np.array(df.values)
                        for rec in X:
                            cell,lon,lat,tim = rec
                            if tim.hour in work_hour:
                                work_cell.append(cell)
                            if tim.hour in home_hour:
                                home_cell.append(cell)
                            usr_tslist.append([lon,lat])
                        work_counter = collections.Counter(work_cell)

                        rog = self.cal_rog(usr_tslist)
                        rog_li.append(rog)
                        if len(work_counter)>1:
                            h_lac,h_cid,w_lac,w_cid,u_typ = self.work_judge(work_counter,h_lac,h_cid,w_lac,w_cid,u_typ,rog,home_cell)
                            
                            rec = ",".join([str(uid),str(h_lac),str(h_cid),str(w_lac),str(w_cid),str(u_typ),str(i)])+"\n"
                            fw.write(rec)
                        cp += 1
                        t1=time.time()
                        if cp%1000 == 0: 
                            print cp,cq,co,t1-t0
                            # import matplotlib.pyplot as plt
                            # # print rog_li
                            # print np.percentile(np.array(rog_li),0.4)
                            # plt.hist(rog_li,20, normed=1)

                            # plt.xlabel("ROG",size=18)
                            # plt.xlim(0,12)
                            # plt.ylabel('Normalized Number',size=18)
                            # plt.ylim(0,0.8)
                            # plt.show()
                            # exit()
                cq+=1
            f.close()
            # fo.close()
        fw.close()

#######################################################
if __name__ == "__main__":
    bat = BATCH()
    tmin,tmax = sys.argv[1],sys.argv[2]
    bat.run(int(tmin),int(tmax))
    del bat