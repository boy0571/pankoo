# coding=utf-8
import MySQLdb 
import pandas as pd 
from collections import Counter
from sqlalchemy import create_engine
db =MySQLdb.connect('localhost','syn_da','syn_da','pankoo')
db.close()
db =MySQLdb.connect('localhost','syn_da','syn_da','pankoo',charset='utf8')
cursor = db.cursor()
cursor.execute('set names utf8')
db.commit()
cursor.execute('drop table if exists part_1 ')
sql1='create temporary table tmp_table select * from transactionrecord04 limit 0,400000'
cursor.execute(sql1)

sql2='alter table tmp_table add BRAND varchar(10) NOT NULL DEFAULT "O"'
cursor.execute(sql2)
sql3='update  tmp_table set BRAND= substring(ITEMCODE,1,1)'
cursor.execute(sql3)
sql4='create table part_1 as select STORE_CODE,BRAND,COUNT(*) as Record_Count from tmp_table group by STORE_CODE,BRAND'
cursor.execute(sql4)
sql6="""create temporary table part_2 select a.STORE_CODE,a.BRAND,a.Record_Count from part_1 a, (select max(Record_Count) as cn,STORE_CODE,BRAND from part_1 group by STORE_CODE) b 
where a.STORE_CODE=b.STORE_CODE and a.Record_Count=b.cn"""
cursor.execute(sql6)
sql7='select * from transactionrecord04 group by BIZDATE,STORE_CODE'
sql8="""create temporary table part_3 select * from 
(select STORE_CODE as STORE_CODE_1,BRAND from part_2 ) tmp1 
inner join 
(select * from transactionrecord04 group by BIZDATE,STORE_CODE) tmp2 
on tmp1.STORE_CODE_1=tmp2.STORE_CODE """
cursor.execute(sql8)
sql9='create temporary table part_4  select BIZDATE,BRAND,COUNT(*) as STORE_CODE_COUNT from part_3 group by BIZDATE,BRAND'
cursor.execute(sql9)
cursor.execute('drop table if exists part_1 ')
sql10="""alter table part_4 add BRAND_1_STORE_CODE_COUNT INT(10) NOT NULL DEFAULT 0,
add BRAND_2_STORE_CODE_COUNT INT(10) NOT NULL DEFAULT 0, add BRAND_6_STORE_CODE_COUNT INT(10) NOT NULL DEFAULT 0"""
cursor.execute(sql10)
sql11="""update part_4 set BRAND_6_STORE_CODE_COUNT=(case WHEN BRAND=6 then STORE_CODE_COUNT else 0 end)"""
sql12="""update part_4 set BRAND_1_STORE_CODE_COUNT=(case WHEN BRAND=1 then STORE_CODE_COUNT else 0 end)"""
sql13="""update part_4 set BRAND_2_STORE_CODE_COUNT=(case WHEN BRAND=2 then STORE_CODE_COUNT else 0 end);"""
cursor.execute(sql11)
cursor.execute(sql12)
cursor.execute(sql13)
sql14="""create temporary table part_5 select BIZDATE,max(BRAND_1_STORE_CODE_COUNT) AS BRAND_1_STORE_CODE_COUNT,
max(BRAND_2_STORE_CODE_COUNT) AS BRAND_2_STORE_CODE_COUNT,max(BRAND_6_STORE_CODE_COUNT) AS BRAND_6_STORE_CODE_COUNT
from part_4 group by BIZDATE"""

sql15='create temporary table part_6 select BIZDATE,ITEMCODE,SUM(QUANTITY) AS RECORD_COUNT from tmp_table  group by BIZDATE,ITEMCODE'
sql16="""alter table part_6 add KEY_MERGE INT(10) NOT NULL DEFAULT 1"""
sql17="""create temporary table part_7 select * from part_6 where BIZDATE='2017-04-30' or BIZDATE='2017-04-29' or BIZDATE='2017-04-28'"""
sql18="""set @i = -1; 
set @sql = repeat(" select 1 union all",-datediff('2014-01-01','2018-01-01')+1);
set @sql = left(@sql,length(@sql)-length(" union all"));
set @sql = concat("create temporary table part_8 select date_add('2014-01-01',interval @i:=@i+1 day) as BIZDATE from (",@sql,") as tmp");
prepare stmt from @sql;
execute stmt;"""
sql19="create temporary table part_9 select  BIZDATE from part_8 where BIZDATE <='2017-07-01' "
sql20='alter table part_9 add KEY_MERGE INT(10) NOT NULL DEFAULT 1'
sql21='create temporary table part_10 select a.BIZDATE ,b.ITEMCODE,b.RECORD_COUNT from part_9 a inner join part_7 b on b.KEY_MERGE=a.KEY_MERGE '
cursor.execute(sql15)
cursor.execute(sql16)
cursor.execute(sql17)
cursor.execute(sql18)
cursor.close()
cursor = db.cursor()
cursor.execute(sql19)
cursor.execute(sql20)
cursor.execute(sql21)
sql22='update part_10 set BIZDATE=DATE_FORMAT(BIZDATE,"%Y-%m-%d")'
cursor.execute(sql22)

sql23='create temporary table part_11 select * from part_10 group by ITEMCODE,RECORD_COUNT,BIZDATE'
sql24='alter table part_11 modify column ITEMCODE varchar(20),modify column RECORD_COUNT INT(38),modify column BIZDATE varchar(20)'
cursor.execute(sql23)
cursor.execute(sql24)
sql25='alter table part_6 drop column KEY_MERGE'
cursor.execute(sql25)
sql26='insert into part_6 select * from part_11'
cursor.execute(sql26)
sql
sql00="""create temporary table test select * from holiday_1 where holiday='国庆'"""
cursor.execute(sql00)
df = pd.read_sql('select * from test',db)
print df
#print Counter(df.BRAND)
#'STR_TO_DATE(bizdate,'%Y-%m-%d') as bizdate'
db.close()
#load data local infile '/home/derek/Documents/pankoo_dmp/tianchi_holiday.csv' into table holiday fields terminated by ',' optionally enclosed by "" lines terminated by '\r\n' ignore 1 lines (date,holiday,legal);
