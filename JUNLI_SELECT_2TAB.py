#coding=utf-8
import os
import  pg


class DATA2DB():

    def __init__(self):
        self.db = pg.DB(host='localhost',port=5432,user='postgres',dbname='bjcellular',passwd='123456')

    def read_files(self,rootdir):
        print 'read files ...'
        filelist = []
        # print rootdir
        for curdir, subfolders, files in os.walk(rootdir):
            for file in files:
                f = os.path.join(curdir,file)
                # print f,os.path.getsize(f)                     #os.path.getsize(f)返回文件大小 （字节为单位）
                #print 
                # fh = open(f)
                # lins = fh.readlines()

                filelist.append(f)

        return filelist


    def merge(self,rootdir,outputfile):
        
        filelist = self.read_files(rootdir)
        # file_num =[]
        fw =open(outputfile,'w')
        for file in filelist:
            print file
            f = open(file,'r')
            for ln in f:
                rec = ln.strip().split(",")
                # if int(rec[1]) == 0:
                #     file_num.append( rec[-1])
                fw.write(ln)
            f.close()
        fw.close()

        # print len(file_num)

    def Create_Table(self,TableName,Fields):
        sql = """
            DROP TABLE IF EXISTS %s;
            CREATE TABLE %s()
            """ % (TableName,TableName)
        self.db.query(sql)

        for field in Fields:
            f,dtype = field
            sql = """
                ALTER TABLE %s ADD COLUMN %s %s;
                """ % (TableName,f,dtype)
            self.db.query(sql)
        print 'table %s created!' %(TableName)

    def Create_Index(self,TableName,Fields):
        # self.db.query("BEGIN")
        for field in Fields:
            f,dtype = field
            inx = TableName + '_' + f + '_inx'
            if dtype == 'geometry':
                sql = """
                    DROP INDEX IF EXISTS %s;
                    CREATE INDEX %s ON %s USING gist(%s);
                    """ % (inx,inx,TableName,f)
            else:
                sql = """
                    DROP INDEX IF EXISTS %s;
                    CREATE INDEX %s ON %s(%s);
                    """ % (inx,inx,TableName,f)
            # print sql
            self.db.query(sql)
            print "%s created!"%(inx)
        # self.db.query("END") 

    def tab_insert(self,TableName,file):
        f =open(file,'r')
        for ln in f:
            imsi,h_lac,h_cid,w_lac,w_cid,u_typ,file_num = [int(r) for r in ln.strip().split(",")]
            sql = "INSERT INTO %s VALUES (%d,%d,%d,%d,%d,%d,%d)"%(TableName,imsi,h_lac,h_cid,w_lac,w_cid,u_typ,file_num)
            self.db.query(sql)
        f.close()


    def run(self):
        # rootdir = r'D:\00_Work\2017YFB0503700\tongqin\data\\'
        # outputfile = "./tongqin_orig_new.csv"
        # self.merge(rootdir,outputfile)

        TableName = "bj_userinfos_fin" 
        TabFields = [('imsi','bigint'),('home_lac','int'),('home_cid','int'),('work_lac','int'),('work_cid','int'),('traffic_type','int'),('file_num','int')
                    ,('home_district_id','int'),('home_community_id','int'),('work_district_id','int'),('work_community_id','int')]
        self.Create_Table(TableName,TabFields)

        exit()
        TableName1 = "bj_userinfos_final_noid" 
        TabFields = [('imsi','bigint'),('home_lac','int'),('home_cid','int'),('work_lac','int'),('work_cid','int'),('traffic_type','int'),('file_num','int')]
                    # ,('home_community_id','int'),('home_district_id','int'),('work_community_id','int'),('work_district_id','int')]
        self.Create_Table(TableName1,TabFields)

        # self.tab_insert(TableName,outputfile)
        
        # self.Create_Index(TableName,TabFields)


        sql = "SELECT * FROM bj_usershw_xy;"
        cp,cq,ct,cn = 0,0,0,0
        for data_imsi in  self.db.query(sql).dictresult():
            imsi,h_lac,h_cid,w_lac,w_cid,u_typ,fn = data_imsi['imsi'],data_imsi['home_lac'],data_imsi['home_cid'],data_imsi['work_lac'],data_imsi['work_cid'],data_imsi['traffic_type'],data_imsi['file_num']
            sql = "select * from bj_usershw where imsi = %s"%imsi
            if len(self.db.query(sql).dictresult()) == 0: 
                if h_lac == w_lac and h_cid == w_cid:
                    u_typ = int(u_typ)-100
                    cn+=1
                sql_in = "INSERT INTO %s VALUES(%d,%d,%d,%d,%d,%d,%d)"%(TableName, imsi,h_lac,h_cid,w_lac,w_cid,u_typ,999 )
                self.db.query(sql_in)
                sql_in = "INSERT INTO %s VALUES(%d,%d,%d,%d,%d,%d,%d)"%(TableName1, imsi,h_lac,h_cid,w_lac,w_cid,u_typ,999 )
                self.db.query(sql_in)
                cq+=1
            else:
                sql_in = "INSERT INTO %s VALUES(%d,%d,%d,%d,%d,%d,%d)"%(TableName1, imsi,h_lac,h_cid,w_lac,w_cid,u_typ,fn)
                self.db.query(sql_in)
                ct+=1

            cp+=1
            if cp%100000 == 0:
                print cp,cq,ct,cn

        # sql = "select * from bj_usershw_xy where home_cid = work_cid and traffic_type > 1100"

if __name__ == '__main__':

    test = DATA2DB()  
    test.run()   