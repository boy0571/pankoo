
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

#inputpath='C:\Users\Chen\Desktop\data\POC_DAY_SUM_COMBINE_140901_all.csv'
#data=pd.read_csv(inputpath,index_col=False)
'''
inputpath='~/Documents/pankoo_pata/pankoo_pa/ready_data_pa_pro_0630.csv'
data=pd.read_csv(inputpath,index_col=False)
data['BIZDATE'] = data['BIZDATE'].apply(pd.Timestamp)
data['start_date'] = data['start_date'].apply(pd.Timestamp)
data['end_date'] = data['end_date'].apply(pd.Timestamp)
#data['duration']=1
#for i in range(len(data)):
 #   data.duration[i]=(data.end_date[i] - data.start_date[i]).days
#print (data.end_date[1] - data.start_date[1]).days
#data['recomm'] = data['recomm'].replace(numcigar)
# now convert the types
#data['recomm'] = data['recomm'].convert_objects(convert_numeric=True)
print len(data),data.info()
print data.describe()
#a=np.array([1,2,3,4,5,0])
#print a.mean()
time0=time.time()
data=data.fillna(0)
itemcode = data['ITEMCODE'].unique()


print itemcode
itemcode = itemcode[itemcode!=1080211]
#print itemcode
#print data['BIZDATE'].unique()

data_shop = data[data['ITEMCODE']==1080211]

data_shop = data_shop.sort(['BIZDATE'],ascending=1)
data_shop=data_shop.reset_index(drop=True)
print len(data_shop)

length = len(data_shop)
duration=min(28,len(data_shop))
for i in range(0,max(len(data_shop),duration) - duration + 1):
    data_shop.loc[i,'target_28']=data_shop.loc[i+1:i+duration,'Record_Count'].mean()
    data_shop.loc[i,'target_28_sum']=data_shop.loc[i+1:i+duration,'Record_Count'].sum()
    data_shop.loc[i,'holiday_28']=data_shop.loc[i+1:i+duration,'holiday'].mean()
    data_shop.loc[i,'comp_28']=data_shop.loc[i+1:i+duration,'comp'].mean()
    data_shop.loc[i,'temp_mean_2week']=data_shop.loc[i+1:i+14,'temp'].mean()
for i in range(0,1):
    data_shop.loc[i,'target_7']=data_shop.loc[i+1:i+7,'Record_Count'].mean()
    data_shop.loc[i,'target_7_sum']=data_shop.loc[i+1:i+7,'Record_Count'].sum()


data_c = data_shop
for a in itemcode:
    print 'a',a
    data_shop = data[data['ITEMCODE']==a]
    print data_shop.shape
    data_shop = data_shop.sort(['BIZDATE'],ascending=1)
    data_shop=data_shop.reset_index(drop=True)
    #print data_shop.loc[28-27:28,'Record_Count_pay']
    length = len(data_shop)
    duration=min(28,len(data_shop))
    for i in range(0,max(len(data_shop),duration) - duration + 1):
        data_shop.loc[i,'target_28']=data_shop.loc[i+1:i+duration,'Record_Count'].mean()
        data_shop.loc[i,'target_28_sum']=data_shop.loc[i+1:i+duration,'Record_Count'].sum()
        data_shop.loc[i,'holiday_28']=data_shop.loc[i+1:i+duration,'holiday'].mean()
        data_shop.loc[i,'comp_28']=data_shop.loc[i+1:i+duration,'comp'].mean()
        data_shop.loc[i,'temp_mean_2week']=data_shop.loc[i+1:i+14,'temp'].mean()
    for i in range(0,1):
    	data_shop.loc[i,'target_7']=data_shop.loc[i+1:i+7,'Record_Count'].mean()
    	data_shop.loc[i,'target_7_sum']=data_shop.loc[i+1:i+7,'Record_Count'].sum()
    data_c=pd.concat([data_c,data_shop])
    print len(data_c)

outputpath='~/Documents/pankoo_pata/pankoo_pa/ready_data_pa_month_0630.csv'
#outputpath='C:\Users\derek\Desktop\\test_data\\all_test_ready_data_month_04.csv'
data_c.to_csv(outputpath)
print 'end',time.time()-time0