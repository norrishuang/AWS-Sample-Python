drop table tripdb.uber_movement_nyc_${NUM} purge;
create table tripdb.uber_movement_nyc_${NUM}
    LOCATION 's3://emr-hive-us-east-1-812046859005/hive/warehouse/tripdb.db/uber_movement_nyc_${NUM}' as
select year(tpep_pickup_datetime) year,month(tpep_pickup_datetime) month,day(tpep_pickup_datetime) day,hour(tpep_pickup_datetime) hour,t.*
from tripdb.ws_taxi t where tpep_pickup_datetime >= '2016-01-01 00:00:00' and tpep_pickup_datetime <= '2022-01-01 00:00:00';

drop table if exists ods.ods_uber_data_hour_01_${NUM} purge;
create table ods.ods_uber_data_hour_01_${NUM} location 's3://emr-hive-us-east-1-812046859005/hive/warehouse/ods.db/ods_uber_data_hour_01_${NUM}'
    as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from tripdb.uber_movement_nyc where hour >= 0 and hour <= 12 group by year,month,day,hour;

drop table if exists ods.ods_uber_data_hour_02_${NUM} purge;
create table ods.ods_uber_data_hour_02_${NUM} location 's3://emr-hive-us-east-1-812046859005/hive/warehouse/ods.db/ods_uber_data_hour_02_${NUM}'
    as select concat(year,"-",lpad(month,2,0),"-",lpad(day,2,0)) pardate,hour,count(*) as cnt
 from tripdb.uber_movement_nyc where hour >= 13 and hour <= 23 group by year,month,day,hour;
----hahaha
----sss
drop table if exists dwd.dwd_uber_data_hour_${NUM} purge;
create table dwd.dwd_uber_data_hour_${NUM} location 's3://emr-hive-us-east-1-812046859005/hive/warehouse/dwd.db/dwd_uber_data_hour_${NUM}'
    as
    select * from ods.ods_uber_data_hour_01_${NUM} where pardate = '${DT}';

insert overwrite table dwd.dwd_uber_data_hour_${NUM}
    select * from ods.ods_uber_data_hour_02_${NUM} where pardate = '${DT}';