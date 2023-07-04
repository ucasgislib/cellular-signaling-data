#coding=utf-8

import calendar
import time,os,glob,string,datetime
import traceback
import math
import threading
import numpy as np,pandas as pd
from pandas import Series,DataFrame
import os,sys,copy
import cPickle as pickle

import pylab
import json
from datetime import datetime,timedelta
from itertools import groupby
from operator import itemgetter

######################################################################

class BATCH():
    # def __init__(self):
    #     # with open('./bj_cellinfo_20150102.pkl', 'rb') as f:
    #     #     self.cell_dict = pickle.load(f)
    #     params = {'proj':'merc','a':6378137,'b':6378137,'lat_ts':0,'lon_0':0,'x_0':0,'y_0':0,'k':1.0,'units':'m','nadgrids':'@null'}
    #     self.proj900913 = Proj(params)

    # def __del__(self):
    #     pass

    def read_mapper_output(self, file, separator='\x01'):
        for line in file:
            yield [line.rstrip().split(separator)[4],line.rstrip().split(separator)]

    def read_class_output(self, file, separator=','):
        for line in file:
            yield [line.rstrip().split(separator)[0],line.rstrip().split(separator)]

    def run(self,tmin,tmax):

        fw = open("imsi_filenum.csv",'w')
        for i in range(tmin,tmax):
            print "===========  %s  =========="%i
            f=open("./part-00%03d"%i,'r')
            data = self.read_mapper_output(f, separator='\x01')
            cp,cq,co = 0,0,0
            t0 = time.time()
            for uid, group in groupby(data, itemgetter(0)):
                fw.write("%s,%d\n"%(uid,i))                
                cp += 1
                t1=time.time()
                if cp%10000 == 0: 
                    print cp,cq,co,t1-t0
                cq+=1
            f.close()
        fw.close()

#######################################################
if __name__ == "__main__":
    bat = BATCH()
    tmin,tmax = sys.argv[1],sys.argv[2]
    bat.run(int(tmin),int(tmax))
    del bat