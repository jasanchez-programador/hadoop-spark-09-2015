														Con estas regiones:

*** LetraRegion,Lat,Long ***
cat /home/training/training_materials/data/status-regions.txt 
A,36.403171752604756,-119.89983102088591
B,44.23449040206693,-121.8004293853695
C,34.18840233263631,-117.68588190183058
D,38.53179844863589,-121.28922634614136
E,35.08592000544959,-112.57643826547951

														sobre estos datos:

hdfs dfs -cat /loudacre/devicestatus_etl_python/part-00000 | head -n 5
*** fecha,fabricante,id,lat,long ***
2014-03-15:10:10:20,Sorrento,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253
2014-03-15:10:10:20,MeeToo,ef8c7564-0a1a-4650-a655-c8bbd5f8f943,37.4321088904,-121.485029632
2014-03-15:10:10:20,MeeToo,23eba027-b95a-4729-9a4b-a3cca51c5548,39.4378908349,-120.938978486
2014-03-15:10:10:20,Sorrento,707daba1-5640-4d60-a6d9-1d6fa0645be0,39.3635186767,-119.400334708
2014-03-15:10:10:20,Ronin,db66fe81-aa55-43b4-9418-fc6e7a00f891,33.1913581092,-116.448242643

															devolver DATO,REGION_MAS_CERCANA:

 
*** fecha,fabricante,id,lat,long,LetraRegion ***
2014-03-15:10:10:20,Sorrento,8cc3b47e-bd01-4482-b500-28f2342679af,33.6894754264,-117.543308253,A

															Codigo:

def distanciaPuntos(p1,p2):
	return ( (p1[0]-p2[0]) ** 2 + (p1[1]-p2[1]) ** 2 )

# producto-> String  ;   regiones -> array[String]
# por cada registro/producto del map:
# --> saco sus coordenadas y las comparo con las coordenadas de cada region
# --> devulvo el producto añadiendo al final la Letra de la Region mas cercana
def calcularRegion(producto, regiones): 

	distancia = float("inf")
	regionFinal = ""

	puntoProducto = []
	puntoProducto.append(float(producto.split(",")[3]))
	puntoProducto.append(float(producto.split(",")[4]))


	for region in regiones:
		regionActual = str(region.split(",")[0])
		puntoRegion = []
		puntoRegion.append(float(region.split(",")[1]))
		puntoRegion.append(float(region.split(",")[2]))
		
		distanciaActual = distanciaPuntos(puntoRegion,puntoProducto)
		if ( distanciaActual < distancia ):
			distancia = distanciaActual
			regionFinal = regionActual

	return producto + "," + regionFinal

rdd_productos = sc.textFile("/loudacre/devicestatus_etl_python/*")
rdd_regiones = sc.textFile("file:/home/training/training_materials/data/status-regions.txt")
# como no puedo pasar rdd como argumento a mi funcion dentro del map
# genero un array con TODAS las regiones
numRegiones = rdd_regiones.count()
array_regiones = rdd_regiones.takeSample(0,numRegiones)
# obtengo de cada producto, el producto junto a la region mas cercana			
rdd_final = rdd_productos.map( lambda reg: calcularRegion(reg,array_regiones) )
# los separo por region para poder guardarlos en su Region/particion
rdd_regionA = rdd_final.filter( lambda reg: reg.endswith("A") )
rdd_regionB = rdd_final.filter( lambda reg: reg.endswith("B") )
rdd_regionC = rdd_final.filter( lambda reg: reg.endswith("C") )
rdd_regionD = rdd_final.filter( lambda reg: reg.endswith("D") )
rdd_regionE = rdd_final.filter( lambda reg: reg.endswith("E") )
# Quito la region del fichero porque estara en el nombre del directorio
rdd_regionA_final = rdd_regionA.map(lambda reg: reg.split(",")).map(lambda reg: (reg[0]+","+reg[1]+","+reg[2]+","+reg[3]+","+reg[4]) )
rdd_regionB_final = rdd_regionB.map(lambda reg: reg.split(",")).map(lambda reg: (reg[0]+","+reg[1]+","+reg[2]+","+reg[3]+","+reg[4]) )
rdd_regionC_final = rdd_regionC.map(lambda reg: reg.split(",")).map(lambda reg: (reg[0]+","+reg[1]+","+reg[2]+","+reg[3]+","+reg[4]) )
rdd_regionD_final = rdd_regionD.map(lambda reg: reg.split(",")).map(lambda reg: (reg[0]+","+reg[1]+","+reg[2]+","+reg[3]+","+reg[4]) )
rdd_regionE_final = rdd_regionE.map(lambda reg: reg.split(",")).map(lambda reg: (reg[0]+","+reg[1]+","+reg[2]+","+reg[3]+","+reg[4]) )

# guardo cada uno en su directorio para poder crear tabla externa en implala
rdd_regionA_final.saveAsTextFile("/loudacre/devicestatus_region/region=A")
rdd_regionB_final.saveAsTextFile("/loudacre/devicestatus_region/region=B")
rdd_regionC_final.saveAsTextFile("/loudacre/devicestatus_region/region=C")
rdd_regionD_final.saveAsTextFile("/loudacre/devicestatus_region/region=D")
rdd_regionE_final.saveAsTextFile("/loudacre/devicestatus_region/region=E")


# crear tabla externa desde impala
$ impala-shell
create schema mod17Bonus;

use mod17Bonus;

create external table dispositivo(fecha string, empresa string, id string, lan float, long float)
partitioned by(region string)
row format delimited
fields terminated by ','
stored as textfile 
location '/loudacre/devicestatus_region'
;

alter table dispositivo add partition(region="A");
alter table dispositivo add partition(region="B");
alter table dispositivo add partition(region="C");
alter table dispositivo add partition(region="D");
alter table dispositivo add partition(region="E");

#desde hive podriamos hacerlo con:
# CUIDADO FALLA A VECES!!
MSCK REPAIR TABLE dispositivo;
