drop table if exists hivemetastore.ods_uber_data_hour_11;
create table hivemetastore.ods_uber_data_hour_11 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 11 group by year,month,day,hour;
drop table if exists hivemetastore.ods_uber_data_hour_12;
create table hivemetastore.ods_uber_data_hour_12 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 12 group by year,month,day,hour;
drop table if exists hivemetastore.ods_uber_data_hour_13;
create table hivemetastore.ods_uber_data_hour_13 as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = 13 group by year,month,day,hour;

drop table if exists hivemetastore.ods_uber_data_hour_test;
create table hivemetastore.ods_uber_data_hour_test as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from hivemetastore.uber_movement_nyc where hour = {HOUR} group by year,month,day,hour;

drop table if exists dwd_uber_data_hour;
create table dwd_uber_data_hour as select * from hivemetastore.ods_uber_data_hour_11 where pardate = '{DT}' 
union select * from hivemetastore.ods_uber_data_hour_12 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_13 where pardate = '{DT}'
union select * from hivemetastore.ods_uber_data_hour_test where pardate = '{DT}';