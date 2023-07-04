#coding=utf-8

import  psycopg2
from pyecharts import Pie

conn = psycopg2.connect(database="bjcellular", user="postgres", host="127.0.0.1", port="5432", password="123456")
cur = conn.cursor()

# #统计每一类的人数
# typ_li = [1001,1002,1003,1004,1101,1102,1103,1104]
# for typ in typ_li:
#   sql = "select count(*) from bj_userinfos where traffic_type  = %d"%typ
#   cur.execute(sql)
#   print typ,cur.fetchall()[0][0]

# 统计每个区每一类的人数
# tabName = "bj_zonestat"
# seqName = tabName + '_id_seq'
# sql = """
#     DROP SEQUENCE IF EXISTS %s CASCADE;
#     CREATE SEQUENCE %s;

#     DROP TABLE IF EXISTS %s;
#     CREATE TABLE %s (
#         district        int,
#         traffic_type    int,
#         home_num        int,
#         work_num        int
#     )
# """ % (seqName,seqName,tabName,tabName)
# cur.execute(sql)
# conn.commit()



for district in dirsrict_li:
    sql = "select sum(work_num) from bj_zonestat WHERE district =%d;"%district
    cur.execute(sql)
    print cur.fetchall()
exit()


dis_name = [u"东城区",u"西城区",u"朝阳区",u"丰台区",u"石景山区",u"海淀区",u"门头沟区",u"房山区",u"通州区",u"顺义区",u"昌平区",u"大兴区",u"怀柔区",u"密云区",u"平谷区",u"延庆区"]
typ_li = [1001,1002,1003,1004,1101,1102,1103,1104]
type_name = [u"无通勤不活动",u"无通勤小范围活动",u"无通勤中小范围活动",u"无通勤大范围活动",u"短距离通勤",u"中短距离通勤",u"长距离通勤",u"超长距离通勤"]

# li = [1406892 , 4230655 , 1686164 , 141843 , 309291 , 1972783 , 1056660 , 30679]
# pie = Pie(u"北京市移动手机用户分布", title_pos='center')
# pie.add("", type_name, li, center=[55, 50], legend_orient='vertical',legend_pos='left',is_label_show=True,is_legend_show=True,is_more_utils = True, radius=[30, 75])#,color=["red","blue","blue","blue","blue","blue","blue","blue"]
# pie.render(u'./pie/pie_北京市移动手机用户分布.html')
# exit()

dirsrict_li = [3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19]
for didx,district in enumerate(dirsrict_li):
    work_li,home_li = [],[]
    for typ in typ_li:
        sql = "select count(*) from bj_userinfos where home_district_id = %d and traffic_type  = %d"%(district,typ)
        cur.execute(sql)
        # print "home",district,typ,
        home_num = cur.fetchall()[0][0]
        home_li.append(home_num)
        sql = "select count(*) from bj_userinfos where work_district_id = %d and traffic_type  = %d"%(district,typ)
        cur.execute(sql)
        # print "work",district,typ,cur.fetchall()[0][0]
        work_num = cur.fetchall()[0][0]
        work_li.append(work_num)

        insert_sql = "INSERT INTO bj_zonestat VALUES(%d,%d,%d,%d)"%(district,typ,home_num,work_num)
        cur.execute(insert_sql)
    conn.commit()

    # from pyecharts.render import make_snapshot
    # from snapshot_selenium import snapshot

    typ_name = [u"工作地",u"居住地"]
    for idx,li in enumerate([work_li,home_li]):
        if idx == 0:
            continue
        attr = type_name #['1001','1002','1003','1004','1101','1102','1103','1104']
        # pie = Pie(u"%s%s_用户分布"%(dis_name[didx],typ_name[idx]), title_pos='center')
        pie = Pie(u"%s_用户类型分布"%(dis_name[didx]), title_pos='center')
        print dis_name[didx],typ_name[idx],li
        pie.add("", attr, li, center=[55, 50], legend_orient='vertical',legend_pos='left',is_label_show=True,is_legend_show=True,is_more_utils = True, radius=[30, 75])#,color=["red","blue","blue","blue","blue","blue","blue","blue"]
        # pie.render_to_file(u'./pie/pie_%s%s.html'%(dis_name[didx],typ_name[idx]))#%dis_name[didx])
        pie.render(u'./pie/pie_%s.html'%(dis_name[didx]))#%dis_name[didx])
        # make_snapshot(snapshot, pie.render(), u'./pie/pie_%s.png'%(dis_name[didx]))
