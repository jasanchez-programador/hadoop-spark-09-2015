use world;

alter table city_part
add partition(countrycode='ITA')
;

insert into table city_part
partition(countrycode='ITA')
select id,name,district,population
from city
where countrycode='ITA'
;
