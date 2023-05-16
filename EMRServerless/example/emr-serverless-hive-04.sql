drop table if exists hivemetastore.ods_uber_data_hour_14;
create table hivemetastore.ods_uber_data_hour_14 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 14 group by year,month,day,hour;
drop table if exists hivemetastore.ods_uber_data_hour_15;
create table hivemetastore.ods_uber_data_hour_15 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 15 group by year,month,day,hour;
drop table if exists hivemetastore.ods_uber_data_hour_16;
create table hivemetastore.ods_uber_data_hour_16 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 16 group by year,month,day,hour;

drop table if exists hivemetastore.ods_uber_data_hour_test;
create table hivemetastore.ods_uber_data_hour_test as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = {HOUR} group by year,month,day,hour;
----hahaha
----sss
drop table if exists dwd_uber_data_hour_02;
create table dwd_uber_data_hour_02 as select * from hivemetastore.ods_uber_data_hour_11 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_14 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_15 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_test where pardate = '{DT}';
s
insert overwrite table dwd_uber_data_hour_02 select * from hivemetastore.ods_uber_data_hour_14 where pardate = '{DT}'
                                   union select * from hivemetastore.ods_uber_data_hour_15 where pardate = '{DT}';