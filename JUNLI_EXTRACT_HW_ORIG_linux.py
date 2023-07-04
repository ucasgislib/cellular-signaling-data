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
# import cPickle as pickle
import json
# import  pg
from datetime import datetime,timedelta
from itertools import groupby
from operator import itemgetter
# from pyproj import Proj
# from geopy.distance import great_circle
# from shapely.geometry  import MultiPoint,Polygon
# from geopy.geocoders  import Nominatim

######################################################################


class BATCH():
    
    def read_mapper_output(self, file, separator='\x01'):
        for line in file:
            yield [line.rstrip().split(separator)[4],line.rstrip().split(separator)]

    def run(self):
        
        imsi_li = []
        fu = open("./tongqin_userinfos.csv",'r')
        for ln in fu:
            imsi = int(ln.strip().split(",")[0])
            imsi_li.append(imsi)

        print (len(imsi_li))

        t0 = time.time()
        fo = open("./tongqin_orig_000000.csv",'w')
        f=open("/home/Ricky/bj_imsitim/p000001",'r')
        data = self.read_mapper_output(f, separator='\x01')
        cp = 0
        for uid, group in groupby(data, itemgetter(0)):
            if len(uid) < 10:
                continue
            if int(uid) in imsi_li:
                for li in list(group):
                    try:
                        tim,lac,cid = datetime.utcfromtimestamp(int(li[1][2])),int(li[1][0]),int(li[1][1])
                        x1,y1 = self.cell_dict[(lac,cid)]["lon"],self.cell_dict[(lac,cid)]["lat"]
                        orig_rec = ",".join([str(uid),str(tim),str(lac),str(cid),str(x1),str(y1)])+"\n"
                        fo.write(orig_rec)
                    except:
                        pass
            cp+=1
            if cp%1000 == 0:
                t1 = time.time()
                print (cp,t1-t0)
        f.close()

#######################################################
if __name__ == "__main__":
    bat = BATCH()
    bat.run()
    del bat