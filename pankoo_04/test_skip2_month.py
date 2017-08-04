
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
__author__ = 'AA'
#from dtw import dtw
from numpy import *
import numpy as np
import pandas as pd
import time
'''
import cx_Oracle
import cx_Oracle
ip = 'localhost'
port = 1521
SID = 'orcl'
dsn_tns = cx_Oracle.makedsn(ip, port, SID)

connection = cx_Oracle.connect('syn_da', 'syn_da', dsn_tns)
query = """ SELECT * FROM "all_test_ready_data_04" """
data = pd.read_sql(query, con=connection)
'''
inputpath='~/Documents/pankoo_04/all_test_ready_data_0531.csv'
data=pd.read_csv(inputpath,index_col=False)
data['BIZDATE'] = data['BIZDATE'].apply(pd.Timestamp)
#data['recomm'] = data['recomm'].replace(numcigar)
# now convert the types
#data['recomm'] = data['recomm'].convert_objects(convert_numeric=True)
print len(data),data.info()
print data['recomm'].unique()
#print data.describe()
#a=np.array([1,2,3,4,5,0])
#print a.mean()
time0=time.time()

data=data.fillna(0)

itemcode = data['ITEMCODE'].unique()
print itemcode
itemcode = itemcode[itemcode!=1010008]
#print itemcode
#print data['BIZDATE'].unique()

data_shop = data[data['ITEMCODE']==1010008]

data_shop = data_shop.sort(['BIZDATE'],ascending=1)
data_shop=data_shop.reset_index(drop=True)
print len(data_shop)
for a in range(365+28,len(data_shop)):
    #print data_shop.loc[a,'BIZDATE']
    data_shop.loc[a,'Record_Count_month_365_past']=data_shop.loc[a-365-27:a-365,'Record_Count'].mean()
    data_shop.loc[a,'Record_Count_month_365_future']=data_shop.loc[a-365+1:a-365+28,'Record_Count'].mean()
    data_shop.loc[a,'Record_Count_year']=data_shop.loc[a-365:a,'Record_Count'].mean()
for c in range(84,len(data_shop)):
    data_shop.loc[c,'Record_Count_3_month']=data_shop.loc[c-84:c,'Record_Count'].mean()
for b in range(28,len(data_shop)):
    data_shop.loc[b,'Record_Count_month']=data_shop.loc[b-27:b,'Record_Count'].mean()
    data_shop.loc[b,'Record_Count_week']=data_shop.loc[b-6:b,'Record_Count'].mean()
    data_shop.loc[b,'Record_Count_2week']=data_shop.loc[b-13:b,'Record_Count'].mean()
    data_shop.loc[b,'holiday_month']=data_shop.loc[b-27:b,'holiday'].mean()
    data_shop.loc[b,'comp_month']=data_shop.loc[b-27:b,'comp'].mean()
    data_shop.loc[b,'brand1_store_count_month']=data_shop.loc[b-27:b,'brand_1_STORE_CODE_Count'].mean()
    data_shop.loc[b,'brand2_store_count_month']=data_shop.loc[b-27:b,'brand_2_STORE_CODE_Count'].mean()
    data_shop.loc[b,'brand6_store_count_month']=data_shop.loc[b-27:b,'brand_6_STORE_CODE_Count'].mean()
    data_shop.loc[b,'package_count_month']=data_shop.loc[b-27:b,'PACKAGE_TODAY'].mean()
    data_shop.loc[b,'takeout_count_month']=data_shop.loc[b-27:b,'TAKEOUT_TODAY'].mean()
    data_shop.loc[b,'SP_count_month']=data_shop.loc[b-27:b,'SP_TODAY'].mean()
    data_shop.loc[b,'temp_mean_month']=data_shop.loc[b-27:b,'temp'].mean()
    data_shop.loc[b,'Spring_Eve_Ever']=data_shop.loc[b-27:b,'Spring_Eve'].max()
    data_shop.loc[b,'menu_photo_month']=data_shop.loc[b-27:b,'photo'].mean()
    data_shop.loc[b,'menu_recomm_month']=data_shop.loc[b-27:b,'recomm'].mean()



for i in range(1,len(data_shop)-56):
    data_shop.loc[i,'target_28']=data_shop.loc[i+57:i+84,'Record_Count'].mean()
    data_shop.loc[i,'holiday_28']=data_shop.loc[i+57:i+84,'holiday'].mean()
    data_shop.loc[i,'comp_28']=data_shop.loc[i+57:i+84,'comp'].mean()
    data_shop.loc[i,'brand1_store_count_28']=data_shop.loc[i+57:i+84,'brand_1_STORE_CODE_Count'].mean()
    data_shop.loc[i,'brand2_store_count_28']=data_shop.loc[i+57:i+84,'brand_2_STORE_CODE_Count'].mean()
    data_shop.loc[i,'brand6_store_count_28']=data_shop.loc[i+57:i+84,'brand_6_STORE_CODE_Count'].mean()
    data_shop.loc[i,'temp_mean_2week']=data_shop.loc[i+1:i+14,'temp'].mean()
    data_shop.loc[i,'package_count_28']=data_shop.loc[i+57:i+84,'PACKAGE_TODAY'].mean()
    data_shop.loc[i,'takeout_count_28']=data_shop.loc[i+57:i+84,'TAKEOUT_TODAY'].mean()
    data_shop.loc[i,'SP_count_28']=data_shop.loc[i+57:i+84,'SP_TODAY'].mean()
    data_shop.loc[i,'Spring_Eve_In']=data_shop.loc[i+57:i+84,'Spring_Eve'].max()
    data_shop.loc[i,'menu_photo_28']=data_shop.loc[i+57:i+84,'photo'].mean()
    data_shop.loc[i,'menu_recomm_28']=data_shop.loc[i+57:i+84,'recomm'].mean()


data_c = data_shop
for a in itemcode:
    print 'a',a
    data_shop = data[data['ITEMCODE']==a]
    data_shop = data_shop.sort(['BIZDATE'],ascending=1)
    data_shop=data_shop.reset_index(drop=True)
    #print data_shop.loc[28-27:28,'Record_Count_pay']
    for a in range(365+28,len(data_shop)):
        #print data_shop.loc[a,'BIZDATE']
        data_shop.loc[a,'Record_Count_month_365_past']=data_shop.loc[a-365-27:a-365,'Record_Count'].mean()
        data_shop.loc[a,'Record_Count_month_365_future']=data_shop.loc[a-365+1:a-365+28,'Record_Count'].mean()
        data_shop.loc[a,'Record_Count_year']=data_shop.loc[a-365:a,'Record_Count'].mean()
    for c in range(84,len(data_shop)):
        data_shop.loc[c,'Record_Count_3_month']=data_shop.loc[c-84:c,'Record_Count'].mean()
    for b in range(28,len(data_shop)):
        data_shop.loc[b,'Record_Count_month']=data_shop.loc[b-27:b,'Record_Count'].mean()
        data_shop.loc[b,'Record_Count_week']=data_shop.loc[b-6:b,'Record_Count'].mean()
        data_shop.loc[b,'Record_Count_2week']=data_shop.loc[b-13:b,'Record_Count'].mean()
        data_shop.loc[b,'holiday_month']=data_shop.loc[b-27:b,'holiday'].mean()
        data_shop.loc[b,'comp_month']=data_shop.loc[b-27:b,'comp'].mean()
        data_shop.loc[b,'brand1_store_count_month']=data_shop.loc[b-27:b,'brand_1_STORE_CODE_Count'].mean()
        data_shop.loc[b,'brand2_store_count_month']=data_shop.loc[b-27:b,'brand_2_STORE_CODE_Count'].mean()
        data_shop.loc[b,'brand6_store_count_month']=data_shop.loc[b-27:b,'brand_6_STORE_CODE_Count'].mean()
        data_shop.loc[b,'package_count_month']=data_shop.loc[b-27:b,'PACKAGE_TODAY'].mean()
        data_shop.loc[b,'takeout_count_month']=data_shop.loc[b-27:b,'TAKEOUT_TODAY'].mean()
        data_shop.loc[b,'SP_count_month']=data_shop.loc[b-27:b,'SP_TODAY'].mean()
        data_shop.loc[b,'temp_mean_month']=data_shop.loc[b-27:b,'temp'].mean()
        data_shop.loc[b,'Spring_Eve_Ever']=data_shop.loc[b-27:b,'Spring_Eve'].max()
        data_shop.loc[b,'menu_photo_month']=data_shop.loc[b-27:b,'photo'].mean()
        data_shop.loc[b,'menu_recomm_month']=data_shop.loc[b-27:b,'recomm'].mean()



    for i in range(1,len(data_shop)-56):
        data_shop.loc[i,'target_28']=data_shop.loc[i+57:i+84,'Record_Count'].mean()
        data_shop.loc[i,'holiday_28']=data_shop.loc[i+57:i+84,'holiday'].mean()
        data_shop.loc[i,'comp_28']=data_shop.loc[i+57:i+84,'comp'].mean()
        data_shop.loc[i,'brand1_store_count_28']=data_shop.loc[i+57:i+84,'brand_1_STORE_CODE_Count'].mean()
        data_shop.loc[i,'brand2_store_count_28']=data_shop.loc[i+57:i+84,'brand_2_STORE_CODE_Count'].mean()
        data_shop.loc[i,'brand6_store_count_28']=data_shop.loc[i+57:i+84,'brand_6_STORE_CODE_Count'].mean()
        data_shop.loc[i,'temp_mean_2week']=data_shop.loc[i+1:i+14,'temp'].mean()
        data_shop.loc[i,'package_count_28']=data_shop.loc[i+57:i+84,'PACKAGE_TODAY'].mean()
        data_shop.loc[i,'takeout_count_28']=data_shop.loc[i+57:i+84,'TAKEOUT_TODAY'].mean()
        data_shop.loc[i,'SP_count_28']=data_shop.loc[i+57:i+84,'SP_TODAY'].mean()
        data_shop.loc[i,'Spring_Eve_In']=data_shop.loc[i+57:i+84,'Spring_Eve'].max()
        data_shop.loc[i,'menu_photo_28']=data_shop.loc[i+57:i+84,'photo'].mean()
        data_shop.loc[i,'menu_recomm_28']=data_shop.loc[i+57:i+84,'recomm'].mean()
    data_c=pd.concat([data_c,data_shop])
    print len(data_c)

outputpath='~/Documents/pankoo_04/all_test_ready_data_skip2_month_04.csv'
#outputpath='C:\Users\derek\Desktop\\test_data\\all_test_ready_data_skip2_month_04.csv'
data_c.to_csv(outputpath)

print 'end',time.time()-time0