use world;

insert into table city_avro
select id,name,countrycode,district,population from city
;
