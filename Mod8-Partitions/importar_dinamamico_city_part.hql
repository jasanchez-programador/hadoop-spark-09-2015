use world;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=500;
set hive.exec.max.dynamic.partitions.pernode=500;

insert into table city_part
partition(countrycode)
select id,name,district,population,countrycode
from city
where countrycode not in ('ESP', 'FRA', 'ITA')
;
