use world;

create table city_avro
stored as avro
location '/user/training/julio/comoAvro'
#tblproperties('avro.schema.url'='hdfs:/path/hdfs')
tblproperties('avro.schema.literal' = '{
"name" : "city",
"type" : "record",
"fields" : [
	{"name":"id", "type":"int"},
	{"name":"name", "type":"string"},
	{"name":"countrycode", "type":"string"},
	{"name":"district", "type":"string"},
	{"name":"population", "type":"int"}
			]
}')
;

insert into table city_avro
select * from city
;
