# -*- coding: utf-8 -*-
import xgboost as xgb 
import pandas as pd
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import numpy as np
from collections import Counter
from datetime import *
import time
# -*- coding: utf-8 -*-
import numpy as np
np.random.seed(1337)
from keras.models import Sequential,Model ,model_from_json
import os
from keras.layers import Dense,Activation,Dropout
from keras.layers import LSTM
from keras.utils import np_utils
from keras import losses
from keras.layers import Input
import keras
from keras.layers.normalization  import BatchNormalization
import pandas as pd
import pinyin
from collections import Counter
def  frange(x,y,jump):
       while x<=y:
              yield x
              x+= jump
inputpath='~/Documents/pankoo_pata/pankoo_nopanota/ready_data_0630_for_model.csv'
data1=pd.read_csv(inputpath,index_col=False)
data1['BIZDATE'] = data1['BIZDATE'].apply(pd.Timestamp)
data1=data1.fillna(0)
data1['CAT']=data1['CAT'].astype(str)
data1['month']=data1['month'].astype(str)
data1['holiday_dif']=data1['holiday_28'] - data1['holiday_month'] 
data1['comp_dif']=data1['comp_28'] - data1['comp_month'] 
data1['photo_dif']=data1['menu_photo_28'] - data1['menu_photo_month'] 
data1['recomm_dif']=data1['menu_recomm_28'] - data1['menu_recomm_month'] 
data1['temp_dif']=data1['temp_mean_2week'] - data1['temp_mean_month']
data1['brand1_store_count_dif']=data1['brand1_store_count_28'] - data1['brand1_store_count_month']
data1['brand2_store_count_dif']=data1['brand2_store_count_28'] - data1['brand2_store_count_month']
data1['brand6_store_count_dif']=data1['brand6_store_count_28'] - data1['brand6_store_count_month']
data1['random']=pd.Series(np.random.randn(len(data1)))
data1=data1[data1.Record_Count_month>5]
#print data1.info()
#print data1.head()

def system_sampling(data,k):
	num=int(len(data)/k)
	#sample = [random.sample(datamat[i*k:(i+1)*k],1) for i in range(num)]
	samples = [data.iloc[i*k+a:i*k+a+1,:] for i in range(num)]
	return pd.concat(samples,axis=0)
data1['brand']='F'
data1['brand'][data1.ITEMCODE>2000000]='X'
data1['brand'][data1.ITEMCODE<1000000]='B'
dummy_columns=['CAT','month']
data1=pd.get_dummies(data1,columns=dummy_columns)
#data_train=data1[data1.BIZDATE==date(2017,5,30)]
#data1=data1[data1.CAT_101==1]
data1=data1[data1.target_28<1.5]
data1=data1[data1.target_28>0.5]
#print data1.shape
#print data_train.ITEMCODE.unique()
data1=data1.reset_index()
columns=['ITEMCODE','BIZDATE', 'Record_Count_2week', 'Record_Count_3_month',
       'Record_Count_month', 'Record_Count_month_365_future',
       'Record_Count_month_365_past', 'Record_Count_week',
       'Record_Count_year', 'brand1_store_count_28',
       'brand1_store_count_month', 'brand2_store_count_28',
       'brand2_store_count_month', 'brand6_store_count_28',
       'brand6_store_count_month', 'brand_1_STORE_CODE_Count',
       'brand_2_STORE_CODE_Count', 'brand_6_STORE_CODE_Count', 'comp_28',
       'comp_month', 'holiday_28', 'holiday_month', 'menu_photo_28',
       'menu_photo_month', 'menu_recomm_28', 'menu_recomm_month',
       'target_28', 'temp_mean_2week', 'temp_mean_month', 'START_DATE',
       'END_DATE', 'start', 'end', 'CAT', 'FINAN', 'TREND', 'month',
       'days_to_next_chuxi', 'days_to_last_chuxi', '环比', '同比', '社会影响_28',
       '社会影响_28_past', 'brand','CAT_101', 'CAT_102', 'CAT_103',
       'CAT_104', 'CAT_105', 'CAT_201', 'CAT_202', 'CAT_203', 'CAT_204',
       'CAT_205', 'CAT_206', 'CAT_610', 'CAT_620', 'CAT_630', 'month_1',
       'month_10', 'month_11', 'month_12', 'month_2', 'month_3',
       'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9']

train_f_columns=['Record_Count_2week','Record_Count_3_month', 'Record_Count_week',
       'temp_mean_2week','TREND', 
       'days_to_next_chuxi','holiday_dif','comp_dif','photo_dif','recomm_dif','temp_dif']

#dummy_columns=['brand']

def splite_train(data,brand,train_time,test_time):
	data_b=data[data.brand==brand]
	data_train=data_b[data_b.BIZDATE<=train_time]
	data_train_label=data_train.target_28
	data_train_data=data_train.loc[:,train_f_columns]
	data_test=data_b[data_b.BIZDATE.isin(test_time)]
	data_test_label=data_test.target_28
	data_test_data=data_test.loc[:,train_f_columns]
	return data_train_data,data_train_label,data_test_data,data_test_label,data_train,data_test

train_date=date(2017,3,1)
test_date=[date(2017,4,30),date(2017,4,15),date(2017,3,30),date(2017,5,30)]
#test_date=[date(2017,4,28)]





f_train_data, f_train_label, f_test_data, f_test_label,f_train,f_test = splite_train(data1,'X',train_date,test_date)

print f_train_data.shape
print f_test_data.shape
print data1.shape
print data1[data1.brand=='F'].shape
print data1.BIZDATE.max()
from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from xgboost.sklearn import XGBRegressor
from sklearn.grid_search import GridSearchCV 
from sklearn import cross_validation,metrics
'''
for learning_rate in frange(0.1,0.1,0.02):
       for n_estimators in frange(200,200,200):
              for max_depth in frange(3,3,10):
                     for min_child_weight in frange(20,20,1):
                            for gamma in frange(1,1,1):
                                   for reg_alpha in frange(1,1,1):
                                          for reg_lambda in frange(1,1,10):
                                                 for subsample in frange(0.5,0.5,0.2):
                                                        for colsample_bytree in frange(1,1,0.2):
                                                               print 'learning_rate',learning_rate
                                                               print 'n_estimators',n_estimators
                                                               print 'gamma',gamma
                                                               print 'max_depth',max_depth
                                                               print 'min_child_weight',min_child_weight
                                                               print 'reg_alpha',reg_alpha
                                                               print 'reg_lambda',reg_lambda
                                                               print 'subsample',subsample
                                                               print 'colsample_bytree',colsample_bytree
                                                               xgb1 = XGBRegressor(
                                                               	 learning_rate =learning_rate,
                                                               	 n_estimators=n_estimators,
                                                               	 max_depth=max_depth,###for brand B , deeper, better. For B, 10 reach mape 8%. 
                                                               	 min_child_weight=min_child_weight,
                                                               	 gamma=gamma,
                                                               	 subsample=subsample,
                                                               	 colsample_bytree=colsample_bytree,
                                                               	 objective= 'reg:linear',
                                                               	 nthread=12,
                                                               	 reg_alpha=reg_alpha,
                                                               	 reg_lambda=reg_lambda,
                                                               	 seed=25)


                                                               xgb1.fit(f_train_data,f_train_label)

                                                               pred_train_label=np.array(xgb1.predict(f_train_data))
                                                               pred_test_label = np.array(xgb1.predict(f_test_data))
                                                               print len(pred_test_label)
                                                               mape=abs(pred_train_label -  f_train_label.values) / (f_train_label.values )
                                                               mape=mape[~np.isnan(mape)]
                                                               #print mape
                                                               print 'train',mape.mean()
                                                               mape=abs(pred_test_label - f_test_label.values) / (f_test_label.values)
                                                               mape_raw=abs(1 - f_test_label.values) / (f_test_label.values)
                                                               mape.sort(axis=0)
                                                               #mape=mape[~np.isnan(mape)]
                                                               print mape
                                                               mape=mape[mape<5]####thre  500%
                                                               #print mape
                                                               print len(mape)
                                                               print 'test', mape.mean()
                                                               print 'raw', mape_raw.mean()

'''
xgb1=AdaBoostRegressor(DecisionTreeRegressor(max_depth=2),n_estimators=300,random_state=np.random.RandomState(1),learning_rate=0.1,
       loss='linear')
xgb1=RandomForestRegressor(n_estimators=1000,max_depth=3,min_samples_split=10)
xgb1.fit(f_train_data,f_train_label)

pred_train_label=np.array(xgb1.predict(f_train_data))
pred_test_label = np.array(xgb1.predict(f_test_data))
print len(pred_test_label)
mape=abs(pred_train_label -  f_train_label.values) / (f_train_label.values )
mape=mape[~np.isnan(mape)]
#print mape
print 'train',mape.mean()
mape=abs(pred_test_label - f_test_label.values) / (f_test_label.values)
mape_raw=abs(1 - f_test_label.values) / (f_test_label.values)
mape.sort(axis=0)
#mape=mape[~np.isnan(mape)]
print mape
mape=mape[mape<5]####thre  500%
#print mape
print len(mape)
print 'test', mape.mean()
print 'raw', mape_raw.mean()

#print pred_test_label
#print f_test_label
#print f_train.head()
#print f_train.BIZDATE.unique()
#print f_test.BIZDATE.unique()
#print f_test.head()

'''
main_input = Input(shape=(19,),name='main_input')
x = Dense(30,activation='relu',use_bias=True,kernel_initializer='glorot_normal')(main_input)
x = Dropout(0.3)(x)
temp =Dense(10,activation='relu',use_bias=True,kernel_initializer='glorot_normal')(x)
x=Dense(40,activation='relu',use_bias=True,kernel_initializer='glorot_normal',)(x)
x = Dropout(0.3)(x)
x=BatchNormalization()(x)
x=Dense(10,activation='relu',use_bias=True,kernel_initializer='glorot_normal')(x)
main_output = Dense(1,activation='linear',name='main_output',kernel_initializer='glorot_normal')(x)
model = Model(inputs=[main_input],outputs=main_output)
rmsprop = keras.optimizers.RMSprop(lr=0.0001)
sgd = keras.optimizers.SGD(lr=0.0001,momentum=0.999,decay=0.05,nesterov=False)
adam = keras.optimizers.Adam(lr=0.0001,beta_1=0.9,beta_2=0.999,epsilon=1e-08)
nadam = keras.optimizers.Nadam(lr=0.002,beta_1=0.9,beta_2=0.999,epsilon=1e-08,schedule_decay=0.004)
learning_rate_reduction=keras.callbacks.ReduceLROnPlateau(monitor='loss',factor=0.5,patience=20,cooldown=4,verbose=1,mode='auto',min_lr=0.00001)
model.compile(optimizer=adam,loss=losses.mape,)
model.fit([f_train_data],f_train_label,batch_size=128,epochs=500,verbose=1,callbacks=[learning_rate_reduction])
predicted = model.predict([f_test_data])
score = model.evaluate([f_test_data],[f_test_label])
print score'''