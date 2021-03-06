# Queremos devolver cuantos usuarios usan bash
######
## PRIMERA SOLUCION:
# MAP -- REDUCE -- SUFFLE AND SORT
######

# Cargo 3 ficheros que son copias del passwd
rddUsuarios = sc.textFile("file:/home/training/julio/Mod14-Particiones/Ficheros/passwd*")

# Solo me quedo con los registros acabados en bash
rddSoloBash = rddUsuarios.filter( lambda u: u.endswith("bash") )

# divido la linea por ":" y devuelvo pares de Clave, valor
# shell , 1
rddUsPorShell = rddUsuarios.map( lambda r : (r.split(":")[6],1) )

# Fase Reduce
# sumo todos los valores para cada clave
rddContUsPorShell = rddUsPorShell.reduceByKey( lambda v1,v2: (v1+v2) )


# Fase shuffle and sort
# 1.- agrupo todos los 1 por cada clave
#rddUsPorShellListaValores = rddUsCampos.groupByKey()
# 2.- ordeno las claves descendente
rddContUsPorShellOrdenado = rddContUsPorShell.sortByKey()


for r in rddContUsPorShellOrdenado.collect(): 
		print r

######
## SEGUNDA SOLUCION:
## MAP -- SUFFLE AND SORT -- REDUCE
######

# Cargo 3 ficheros que son copias del passwd
rddUsuarios = sc.textFile("file:/home/training/julio/Mod14-Particiones/Ficheros/passwd*")

# Solo me quedo con los registros acabados en bash
rddSoloBash = rddUsuarios.filter( lambda u: u.endswith("bash") )

# divido la linea por ":" y devuelvo pares de Clave, valor
# shell , 1
rddUsPorShell = rddSoloBash.map( lambda r : (r.split(":")[6],1) )

# Fase shuffle and sort
# 1.- agrupo todos los 1 por cada clave
rddUsPorShellListaValores = rddUsPorShell.groupByKey()
# 2.- ordeno las claves descendente
rddContUsPorShellOrdenado = rddUsPorShellListaValores.sortByKey()

# Fase reduce
rddContShell = rddContUsPorShellOrdenado.mapValues( sum )


for r in rddContShell.collect():
	print r


