
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
__author__ = 'AA'
#from dtw import dtw
from numpy import *
import numpy as np
import pandas as pd
import time
from multiprocessing import process,Manager
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
inputpath='~/Documents/pankoo_pata/pankoo_nopanota/ready_data_NOPANOTA_0630_copy'
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

time1=time.time()

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
for b in range(28,len(data_shop)):
    data_shop.loc[b,'Record_Count_month']=data_shop.loc[b-27:b,'Record_Count'].mean()
    data_shop.loc[b,'Record_Count_week']=data_shop.loc[b-6:b,'Record_Count'].mean()
    data_shop.loc[b,'Record_Count_2week']=data_shop.loc[b-13:b,'Record_Count'].mean()
    data_shop.loc[b,'holiday_month']=data_shop.loc[b-27:b,'holiday'].mean()
    data_shop.loc[b,'comp_month']=data_shop.loc[b-27:b,'comp'].mean()
    data_shop.loc[b,'brand1_store_count_month']=data_shop.loc[b-27:b,'brand_1_STORE_CODE_Count'].mean()
    data_shop.loc[b,'brand2_store_count_month']=data_shop.loc[b-27:b,'brand_2_STORE_CODE_Count'].mean()
    data_shop.loc[b,'brand6_store_count_month']=data_shop.loc[b-27:b,'brand_6_STORE_CODE_Count'].mean()
    data_shop.loc[b,'temp_mean_month']=data_shop.loc[b-27:b,'temp'].mean()
    data_shop.loc[b,'menu_photo_month']=data_shop.loc[b-27:b,'photo'].mean()
    data_shop.loc[b,'menu_recomm_month']=data_shop.loc[b-27:b,'recomm'].mean()
for c in range(84,len(data_shop)):
    data_shop.loc[c,'Record_Count_3_month']=data_shop.loc[c-84:c,'Record_Count'].mean()



for i in range(0,len(data_shop)-28):
    data_shop.loc[i,'target_28']=data_shop.loc[i+1:i+28,'Record_Count'].mean()
    data_shop.loc[i,'holiday_28']=data_shop.loc[i+1:i+28,'holiday'].mean()
    data_shop.loc[i,'comp_28']=data_shop.loc[i+1:i+28,'comp'].mean()
    data_shop.loc[i,'brand1_store_count_28']=data_shop.loc[i+1:i+28,'brand_1_STORE_CODE_Count'].mean()
    data_shop.loc[i,'brand2_store_count_28']=data_shop.loc[i+1:i+28,'brand_2_STORE_CODE_Count'].mean()
    data_shop.loc[i,'brand6_store_count_28']=data_shop.loc[i+1:i+28,'brand_6_STORE_CODE_Count'].mean()
    data_shop.loc[i,'temp_mean_2week']=data_shop.loc[i+1:i+14,'temp'].mean()
    data_shop.loc[i,'menu_photo_28']=data_shop.loc[i+1:i+28,'photo'].mean()
    data_shop.loc[i,'menu_recomm_28']=data_shop.loc[i+1:i+28,'recomm'].mean()

manager = Manager()
dicts={}
dicts= manager.dict()
dicts['length']=0
dicts['lock']=0
data_c = data_shop
print data_c.head()
def row(itemcode):
    global data_c
    for a in itemcode:
        print 'a',a
        data_shop = data[data['ITEMCODE']==a]
        print data_shop.shape
        data_shop = data_shop.sort(['BIZDATE'],ascending=1)
        data_shop=data_shop.reset_index(drop=True)
        #print data_shop.loc[28-27:28,'Record_Count_pay']
        for a in range(365+28,len(data_shop)):
            #print data_shop.loc[a,'BIZDATE']
            data_shop.loc[a,'Record_Count_month_365_past']=data_shop.loc[a-365-27:a-365,'Record_Count'].mean()
            data_shop.loc[a,'Record_Count_month_365_future']=data_shop.loc[a-365+1:a-365+28,'Record_Count'].mean()
            data_shop.loc[a,'Record_Count_year']=data_shop.loc[a-365:a,'Record_Count'].mean()
        for b in range(28,len(data_shop)):
            data_shop.loc[b,'Record_Count_month']=data_shop.loc[b-27:b,'Record_Count'].mean()
            data_shop.loc[b,'Record_Count_week']=data_shop.loc[b-6:b,'Record_Count'].mean()
            data_shop.loc[b,'Record_Count_2week']=data_shop.loc[b-13:b,'Record_Count'].mean()
            data_shop.loc[b,'holiday_month']=data_shop.loc[b-27:b,'holiday'].mean()
            data_shop.loc[b,'comp_month']=data_shop.loc[b-27:b,'comp'].mean()
            data_shop.loc[b,'brand1_store_count_month']=data_shop.loc[b-27:b,'brand_1_STORE_CODE_Count'].mean()
            data_shop.loc[b,'brand2_store_count_month']=data_shop.loc[b-27:b,'brand_2_STORE_CODE_Count'].mean()
            data_shop.loc[b,'brand6_store_count_month']=data_shop.loc[b-27:b,'brand_6_STORE_CODE_Count'].mean()
            data_shop.loc[b,'temp_mean_month']=data_shop.loc[b-27:b,'temp'].mean()
            data_shop.loc[b,'menu_photo_month']=data_shop.loc[b-27:b,'photo'].mean()
            data_shop.loc[b,'menu_recomm_month']=data_shop.loc[b-27:b,'recomm'].mean()
        for c in range(84,len(data_shop)):
            data_shop.loc[c,'Record_Count_3_month']=data_shop.loc[c-84:c,'Record_Count'].mean()


        for i in range(0,len(data_shop)-28):
            data_shop.loc[i,'target_28']=data_shop.loc[i+1:i+28,'Record_Count'].mean()
            data_shop.loc[i,'holiday_28']=data_shop.loc[i+1:i+28,'holiday'].mean()
            data_shop.loc[i,'comp_28']=data_shop.loc[i+1:i+28,'comp'].mean()
            data_shop.loc[i,'brand1_store_count_28']=data_shop.loc[i+1:i+28,'brand_1_STORE_CODE_Count'].mean()
            data_shop.loc[i,'brand2_store_count_28']=data_shop.loc[i+1:i+28,'brand_2_STORE_CODE_Count'].mean()
            data_shop.loc[i,'brand6_store_count_28']=data_shop.loc[i+1:i+28,'brand_6_STORE_CODE_Count'].mean()
            data_shop.loc[i,'temp_mean_2week']=data_shop.loc[i+1:i+14,'temp'].mean()
            data_shop.loc[i,'menu_photo_28']=data_shop.loc[i+1:i+28,'photo'].mean()
            data_shop.loc[i,'menu_recomm_28']=data_shop.loc[i+1:i+28,'recomm'].mean()
        data_c=pd.concat([data_c,data_shop])
        dicts['length']=dicts['length']+len(data_c)
        print dicts['length']
    print itemcode[1], 'computing is over'
    while True:
        if dicts['lock']==1:
            time.sleep(random.random()*10)
            continue
        else:
            dicts['lock']=1
            outputpath='~/Documents/pankoo_pata/pankoo_nopanota/multi_ready_data_nopata_month_0630.csv'
            #outputpath='C:\Users\derek\Desktop\\test_data\\all_test_ready_data_month_04.csv'
            time0=time.time()
            data_c.to_csv(outputpath,mode='a+')
            print itemcode[1],' writing is over',time.time()-time0
            dicts['lock']=0
            break
    
    #outputpath='C:\Users\derek\Desktop\\test_data\\all_test_ready_data_month_04.csv'
    #data_c.to_csv(outputpath,mode='a+')


def startend(num,thread):
    num=num*1.0
    a = 1.0*num*num /2 /thread
    print a
    '''
    a1 = (sqrt(num**2-2*a))
    a2 = (sqrt(a1**2-2*a))
    a3 = (sqrt(a2**2-2*a))
    a4 = (sqrt(a3**2-2*a))
    a5 = (sqrt(a4**2-2*a))
    a6 = (sqrt(a5**2-2*a))
    a7 = (sqrt(a6**2-2*a))
    a8 = (sqrt(a7**2-2*a))
    '''
    a1 = num - num*1.0/thread*1
    a2 = num - num*1.0/thread*2
    a3 = num - num*1.0/thread*3
    a4 = num - num*1.0/thread*4
    a5 = num - num*1.0/thread*5
    a6 = num - num*1.0/thread*6
    a7 = num - num*1.0/thread*7
    a8 = num - num*1.0/thread*8
    #a9 = (sqrt(a8**2-2*a))
    #a10 = (sqrt(a9**2-2*a))
    #a11 = (sqrt(a10**2-2*a))
    #print a9**2-2*a
    #a10 = (sqrt(a9**2-2*a))
    return [int(a1),int(a2),int(a3),int(a4),int(a5),int(a6),int(a7),int(a8)]

length = len(itemcode)
anum = startend(length,9)

from multiprocessing import Process
p_list=[]
time1=time.time()
p= Process(target=row, args=(itemcode[anum[0]:length-1],))
p.daemon=True
p_list.append(p)
for i in range(0,7,1):
    p = Process(target=row, args=(itemcode[anum[i+1]:anum[i]],))
    p.daemon=True
    p_list.append(p)

p = Process(target=row, args=(itemcode[0:anum[7]],))
p.daemon=True
p_list.append(p)
for p in p_list:
    p.start()
for p in p_list:
    p.join()

print 'main process end'
print time.time()-time1