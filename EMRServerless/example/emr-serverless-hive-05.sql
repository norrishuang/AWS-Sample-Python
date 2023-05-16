drop table if exists hivemetastore.ods_uber_data_hour_17;
create table hivemetastore.ods_uber_data_hour_17 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 17 group by year,month,day,hour;
drop table if exists hivemetastore.ods_uber_data_hour_18;
create table hivemetastore.ods_uber_data_hour_18 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 18 group by year,month,day,hour;
drop table if exists hivemetastore.ods_uber_data_hour_19;
create table hivemetastore.ods_uber_data_hour_19 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 19 group by year,month,day,hour;

drop table if exists hivemetastore.ods_uber_data_hour_test;
create table hivemetastore.ods_uber_data_hour_test as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = {HOUR} group by year,month,day,hour;
----hahaha
----sss
drop table if exists dwd_uber_data_hour_03;
create table dwd_uber_data_hour_03 as select * from hivemetastore.ods_uber_data_hour_11 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_17 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_18 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_test where pardate = '{DT}';
s
insert overwrite table dwd_uber_data_hour_03 select * from hivemetastore.ods_uber_data_hour_17 where pardate = '{DT}'
                                   union select * from hivemetastore.ods_uber_data_hour_18 where pardate = '{DT}';