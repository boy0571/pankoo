# coding=utf-8
import MySQLdb 
import pandas as pd 
db =MySQLdb.connect('localhost','syn_da','syn_da','pankoo')
db.close()
db =MySQLdb.connect('localhost','syn_da','syn_da','pankoo')
cursor = db.cursor()
sql = 'create temporary table tmp_table select * from transactionrecord04 \
where (PARENTID <> " " and substring(parentid,1,1) in ("1","2"))'
sql0 = 'select * from item_name limit 0,10'
cursor.execute(sql)
####first table (parentid itemcode)
sql1 = 'create temporary table tmp_1 select ITEMCODE,PARENTID from \
(select distinct PARENTID,ITEMCODE from tmp_table) tmp'
cursor.execute(sql1)
### second table (parentid store_count)
sql2 = 'create temporary table tmp_2 select PARENTID,count(*) as STORE_COUNT from \
(select distinct STORE_CODE,PARENTID from tmp_table) tmp group by PARENTID'
cursor.execute(sql2)
####third table (parentid sales_count)
sql3 = ' create temporary table tmp_3 select PARENTID,count(*) as SALES_COUNT from \
(select any_value(parentid) as parentid from tmp_table group by transaction) tmp group by PARENTID'
cursor.execute(sql3)
#### fourth table parentid bizdate_min, bizdate_max, duration
sql4 = 'create temporary table tmp_4 select count(*),bizdate,parentid from \
(select * from tmp_table group by transaction) tmp  group by parentid,bizdate having count(*)>10 '
cursor.execute(sql4)
sql5 = 'create temporary table tmp_5 select cast(bizdate as date) as bizdate,parentid from \
(select parentid as parentid1, bizdate as bizdate1 from tmp_table group by bizdate,parentid) tmp \
join tmp_4 where \
tmp.bizdate1=tmp_4.bizdate and tmp.parentid1=tmp_4.parentid'
cursor.execute(sql5)
sql6='create temporary table tmp_6 select parentid,max(bizdate) as BIZDATE_Max,\
min(bizdate) as BIZDATE_Min,count(*) as DURATION from tmp_5 group by parentid'
cursor.execute(sql6)
####fifth table item_name
### part1 table 
sql7='create temporary table part_1 select tmp_1.PARENTID,tmp_1.ITEMCODE,tmp_2.STORE_COUNT,tmp_3.SALES_COUNT,tmp_6.BIZDATE_Max,\
tmp_6.BIZDATE_Min,tmp_6.DURATION,item_name.itemname from tmp_1 inner join tmp_2 on tmp_1.PARENTID = tmp_2.PARENTID \
inner join tmp_3 on tmp_1.PARENTID=tmp_3.PARENTID inner join tmp_6 on tmp_1.PARENTID = tmp_6.PARENTID \
inner join item_name on tmp_1.PARENTID = item_name.itemcode'
cursor.execute(sql7)
### add brand and filter with store_count
sql8='alter table part_1 add BRAND varchar(10) NOT NULL DEFAULT "F"'
cursor.execute(sql8)
sql9='update  part_1 set BRAND=(case WHEN substring(PARENTID,1,1)=1 then "F" \
WHEN substring(PARENTID,1,1)=2 then "X" WHEN substring(PARENTID,1,1)=6 then "B" else "O" end) \
where (BRAND="X" and STORE_COUNT>100) or  (BRAND="F" and STORE_COUNT>25)'
cursor.execute(sql9)
sql10='alter table part_1 add MER_KEY varchar(10) NOT NULL DEFAULT "1"'
cursor.execute(sql10)
sql11='update part_1 set BIZDATE_Max="2017-04-26" where PARENTID="2100448" or PARENTID="2100449"'
cursor.execute(sql11)
sql12='create temporary table part_2 select * from part_1 group by PARENTID'
cursor.execute(sql12)

sql13="""set @i = -1; 
set @sql = repeat(" select 1 union all",-datediff('2014-01-01','2018-01-01')+1);
set @sql = left(@sql,length(@sql)-length(" union all"));
set @sql = concat("create temporary table part_3 select date_add('2014-01-01',interval @i:=@i+1 day) as BIZDATE from (",@sql,") as tmp");
prepare stmt from @sql;
execute stmt;"""
cursor.execute(sql13)
cursor.close()
cursor = db.cursor()
sql14="create temporary table part_4 select  BIZDATE from part_3 where BIZDATE <='2017-07-01' "
cursor.execute(sql14)
sql15='alter table part_4 add MER_KEY_TIME varchar(10) NOT NULL DEFAULT "1"'
cursor.execute(sql15)
sql16="""create temporary table part_5 select * from part_2 inner join part_4 on part_2.MER_KEY=part_4.MER_KEY_TIME"""
cursor.execute(sql16)
sql17="""update part_5 set BIZDATE_Max=str_to_date(BIZDATE_Max,'%Y-%m-%d')"""
cursor.execute(sql17)
sql18="""update part_5 set BIZDATE_Min=str_to_date(BIZDATE_Min,'%Y-%m-%d')"""
cursor.execute(sql18)
sql19="""create temporary table part_6 select * from part_5 where BIZDATE>=BIZDATE_Min"""
cursor.execute(sql19)
sql20="""create temporary table part_7 select * from part_6 where BIZDATE_Max>=BIZDATE or datediff(BIZDATE_Max,'2018-01-01')<=2"""
cursor.execute(sql20)
sql21 = 'alter table part_7 drop column ITEMCODE'
cursor.execute(sql21)
sql22='create temporary table part_8 select * from part_7 inner join (select ITEMCODE,PARENTID as PARENTID_1 from part_1) tmp1 on tmp1.PARENTID_1=part_7.PARENTID order by BIZDATE,PARENTID,ITEMCODE'
cursor.execute(sql22)
cursor.execute('drop table if exists 0430PACKAGE_MAP ')
sql23="""create table 0430PACKAGE_MAP as select BIZDATE,ITEMCODE,count(*) as PACKAGE_TODAY from 
(select * from part_8 where instr(ITEMNAME,'外')=0 and (substring(PARENTID,1,3)=210 or substring(PARENTID,1,3)=108)) tmp
group by BIZDATE,ITEMCODE"""
cursor.execute(sql23)
cursor.execute('drop table if exists 0430SP_MAP ')
sql24="""create table 0430SP_MAP as select BIZDATE,ITEMCODE,count(*) as SP_TODAY from 
(select * from part_8 where substring(PARENTID,1,3)=208 or substring(PARENTID,1,3)=106) tmp
group by BIZDATE,ITEMCODE"""
cursor.execute(sql24)
cursor.execute('drop table if exists 0430TAKEOUT_MAP ')
sql25="""create table 0430TAKEOUT_MAP as select BIZDATE,ITEMCODE,count(*) as TAKEOUT_TODAY from 
(select * from part_8 where instr(ITEMNAME,'外')>0 and instr(ITEMNAME,'外带')=0) tmp
group by BIZDATE,ITEMCODE"""
cursor.execute(sql25)
sql00='select * from 0430TAKEOUT_MAP'
df = pd.read_sql(sql00,db)
print df
#'STR_TO_DATE(bizdate,'%Y-%m-%d') as bizdate'
db.close()
#load data local infile '/var/lib/mysql-files/item_name.csv' into table item_name fields terminated by ',' optionally enclosed by "" lines terminated by '\r\n' ignore 1 lines (itemcode,name);
#外双