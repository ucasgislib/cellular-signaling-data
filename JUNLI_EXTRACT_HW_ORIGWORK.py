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


    def read_mapper_output(self, file, separator=','):
        for line in file:
            yield [line.rstrip().split(separator)[0],line.rstrip().split(separator)]

    def myfindidx(self,x,y):
        return [ a for a in range(len(y)) if y[a] == x]

    def cal_Eu_dist(self,x1,y1,x2,y2):
        x,y = x2-x1, y2-y1
        tda = math.sqrt(x**2+y**2)

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

    def work_judge(self,work_counter,h_lac,h_cid,w_lac,w_cid,u_typ,rog,home_counter):
        p40,p60,p90 = 0.5,2,5
        try:
            key_li,values_li = work_counter.keys(),work_counter.values()
            val_max = max(values_li)
            idx = random.choice(self.myfindidx(val_max,values_li))
            cell_max = key_li[idx]
            w_lac,w_cid = cell_max
        except:
            w_lac,w_cid = 0,0

        h_lac,h_cid,w_lac,w_cid = int(h_lac),int(h_cid),int(w_lac),int(w_cid)
        
        if h_lac == 0 or h_cid == w_cid:
            try:
                home_key_li,home_values_li = home_counter.keys(),home_counter.values()
                home_val_max = max(home_values_li)
                home_idx = random.choice(self.myfindidx(home_val_max,home_values_li))
                home_cell_max = home_key_li[idx]
                h_lac,h_cid = home_cell_max
            except:
                pass

        h_lac,h_cid,w_lac,w_cid = int(h_lac),int(h_cid),int(w_lac),int(w_cid)
        if h_lac == 0:
            return 0,0,0,0,0000

        if h_lac == 0 and w_lac != 0:
            if  float(rog) <=p40:
                u_typ = 1001
            elif  p40< float(rog) <= p60:
                u_typ = 1002
            elif  p60< float(rog) <= p90:
                u_typ = 1003
            else:
                u_typ = 1004
        
        if h_cid == w_cid or w_cid == 0:
            # if h_cid == w_cid:
                # self.hwsame_+=1
            if  float(rog) <=p40:
                u_typ = 1001
            elif  p40< float(rog) <= p60:
                u_typ = 1002
            elif  p60< float(rog) <= p90:
                u_typ = 1003
            else:
                u_typ = 1004
        else:
            try:
                hx,hy,wx,wy = self.cell_dict[(h_lac,h_cid)]['x'],self.cell_dict[(h_lac,h_cid)]['y'],self.cell_dict[(w_lac,w_cid)]['x'],self.cell_dict[(w_lac,w_cid)]['y']
                dist = self.cal_Eu_dist(hx,hy,wx,wy)/1000.00
            except:
                print h_lac,h_cid,w_lac,w_cid
                return 0,0,0,0,0000

            # self.dist_li.append(dist/1000.00)
            tq1,tq2,tq3,tq4 = 0.98,3.9,17.4,42.5
            if 0< float(dist) <=tq2:
                u_typ = 1101
            elif  tq2< float(dist) <= tq3:
                u_typ = 1102
            elif  tq3< float(dist) <= tq4:
                u_typ = 1103
            else:
                u_typ = 1104

        return h_lac,h_cid,w_lac,w_cid,u_typ

    def run(self):
        t0 = time.time()
        work_hour = [9,10,11,14,15,16]
        home_hour = [23,24,0,1,2,3]
        # imsi_filenum_dict = {}
        # f = open("imsi_filenum.csv",'r')
        # for ln in f:
        #     try:
        #         imsi,file_num = [int(r) for r in ln.strip().split(",")]
        #         imsi_filenum_dict[imsi] = file_num
        #     except:
        #         print ln
        # fe = open("./pkl/imsi_filenum.pkl",'wb')
        # pickle.dump(imsi_filenum_dict,fe)

        # f = open('./pkl/imsi_filenum.pkl', 'rb')
        # imsi_filenum_dict = pickle.load(f)       
                

        # query_sql = "select * from user_infos  where h_cid <> w_cid;"
        # que_res = self.db.query(query_sql).dictresult()
        # print len(que_res)

        # fe = open("./pkl/que_res.pkl",'wb')
        # pickle.dump(que_res,fe)

        # f = open('./pkl/que_res.pkl', 'rb')
        # que_res = pickle.load(f)      

        fu =open("../tongqin/userinfos_tongqin_new.csv",'w')
        f=open("../tongqin/tongqin_orig_new.csv",'r')
        data = self.read_mapper_output(f, separator=',')
        cp = 0
        for uid, group in groupby(data, itemgetter(0)):
            # if int(uid) == imsi:
            #     usr_tsdict = {}
            #     usr_tsdict["uid"]=uid
            #     cn = 0
            #     for li in list(group):
            #         try:
            #             tim,lac,cid = datetime.utcfromtimestamp(int(li[1][2])),int(li[1][0]),int(li[1][1])
            #             x1,y1 = self.cell_dict[(lac,cid)]["lon"],self.cell_dict[(lac,cid)]["lat"]
            #             orig_rec = ",".join([str(uid),str(tim),str(lac),str(cid),str(x1),str(y1)])+"\n"
            #             fo.write(orig_rec)
            #             if cn ==0:
            #                 usr_tsdict["cell"]=[(lac,cid)]
            #                 usr_tsdict["lon"]=[x1]
            #                 usr_tsdict["lat"]=[y1]
            #                 usr_tsdict["tim"]=[tim]
            #             else:
            #                 usr_tsdict["cell"].append((lac,cid))
            #                 usr_tsdict["lon"].append(x1)
            #                 usr_tsdict["lat"].append(y1)
            #                 usr_tsdict["tim"].append(tim)
            #         except:
            #             pass
            #         cn+=1
            #     usr_tslist,work_cell,home_cell = [],[],[]
            #     df = DataFrame(usr_tsdict,columns=["cell","lon","lat","tim"])
            #     X = np.array(df.values)
            #     for rec in X:
            #         cell,lon,lat,tim = rec
            #         if tim.hour in work_hour:
            #             work_cell.append(cell)
            #         if tim.hour in home_hour:
            #             home_cell.append(cell)
            #         usr_tslist.append([lon,lat])
            #     work_counter = collections.Counter(work_cell)
            #     home_counter = collections.Counter(home_cell)

            #     rog = self.cal_rog(usr_tslist)
            #     if len(work_counter)>1:
            #         h_lac,h_cid,w_lac,w_cid,u_typ = self.work_judge(work_counter,h_lac,h_cid,w_lac,w_cid,u_typ,rog,home_counter)
            #         rec = ",".join([str(uid),str(h_lac),str(h_cid),str(w_lac),str(w_cid),str(u_typ),str(500)])+"\n"
            #         fu.write(rec)
            #     f.close()
            cp+=1
            if cp%100 == 0:
                t1 = time.time()
                print cp,t1-t0
        print cp
#######################################################
if __name__ == "__main__":
    bat = BATCH()

    bat.run()
    del bat