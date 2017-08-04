
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
__author__ = 'AA'
#from dtw import dtw
from numpy import *
import numpy as np
import pandas as pd
import time

inputpath='~/Documents/pankoo_04/all_test_ready_data_onlyrecord_0531.csv'
data=pd.read_csv(inputpath,index_col=False)
#data['BIZDATE'] = data['BIZDATE'].apply(pd.Timestamp)

print len(data),data.info()

time0=time.time()

data=data.fillna(0)

itemcode = data['ITEMCODE'].unique()

print itemcode

print data.head()
for a in itemcode:
    print 'a',a
    data_shop = data[data['ITEMCODE']==a]
    print data_shop.shape
    if len(data_shop)==0:
        continue
    start = pd.Timestamp(data['BIZDATE'].apply(pd.Timestamp).min())
    end = pd.Timestamp(data['BIZDATE'].apply(pd.Timestamp).max())
    data_shop = data_shop.sort(['BIZDATE'],ascending=1)
    data_shop=data_shop.reset_index(drop=True)
######

    for i in pd.date_range(start,end):
        b=data_shop['BIZDATE'].tolist()
        if not str(i)[:10] in b:
            print a,i
            #newdata=pd.Series([id,i,0])
            newdata=pd.DataFrame({'ITEMCODE':a,'BIZDATE':i,'Record_Count':0},index=[1])
            #print newdata
            data=data.append(newdata,ignore_index=True)
#####

outputpath='~/Documents/pankoo_04/all_test_ready_data_onlyrecord_0531_comp.csv'
#outputpath='C:\Users\derek\Desktop\\test_data\\all_test_ready_data_month_04.csv'
data.to_csv(outputpath,encoding='utf-8')
print 'end'