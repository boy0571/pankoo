# -*- coding: utf-8 -*-
__author__ = 'AA'
import numpy as np
import pandas as pd
import time
import datetime
from pypinyin import pinyin,lazy_pinyin
inputpath='E:\\tianchi\user_view_day'
data=pd.read_csv(inputpath,encoding='gbk',index_col=None)
print data.info()
#citynames = data['city_name'].unique()
date_time = data['time_stamp'].apply(pd.Timestamp)

#grouped_shop = data.groupby('shop_id')
#print len(grouped_shop)
#a=grouped_shop.get_group(2)['time_stamp'].tolist()
#data = data[data.shop_id==1].iloc[1:20,]
time1=time.time()

for id in range(1,2001):
    print id
    dataid=data[data.shop_id==id]
    if len(dataid)==0:
        continue
    start = pd.Timestamp(dataid['time_stamp'].min())
    print len(dataid),start
    #print len(data)
    for i in pd.date_range(start,pd.to_datetime('2016-10-31')):
        a=dataid['time_stamp'].tolist()
        if not str(i)[:10] in a:
            newdata=pd.Series([id,i,0])
            newdata=pd.DataFrame({'shop_id':id,'time_stamp':i,'Record_Count':0},index=[1])
            #print newdata
            data=data.append(newdata,ignore_index=True)
    #print len(data[data.shop_id==id])
    #print len(data)
#print data
print time.time()-time1
outputpath='E:\\tianchi\\tianchi\\user_view_day_append_date.csv'
#outputpath='/home/samael/Desktop/tianchi/city_name_pinyin.csv'
data.to_csv(outputpath,encoding='utf-8')
'''
for i in pd.date_range(None,pd.to_datetime('2016-10-31')):
    print i
    '''