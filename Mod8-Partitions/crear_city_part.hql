use world;

create table city_part(id int, name string, district string, population int)
partitioned by (countrycode string)
row format delimited
fields terminated by '\t'
stored as textfile;
