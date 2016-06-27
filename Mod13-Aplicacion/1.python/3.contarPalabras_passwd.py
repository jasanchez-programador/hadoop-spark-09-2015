# spark-submit --master local 3.contarPalabras_passwd.py file:/tmp/passwd
import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
	# Si el faltan argumentos a ejecutar SALIR con ERROR
	if len(sys.argv) < 1:
		print >> sys.stderr, "ERROR: faltan argumentos"
		exit(-1)

    	sconf = SparkConf()
    	sconf.setAppName("Contar Palabras Passwd python")
    	sconf.set("spark.ui.port","4141")
    
    	sc = SparkContext(conf=sconf)

	"""
	aqui ira nuestro programa
	"""
	def sumar(valores):
		suma = 0
		for valor in valores:
			suma += valor
		return suma

	# fichero
	rdd_pwd = sc.textFile(sys.argv[1])

	# map --> palabra,1
	import re
	rdd_par_map = rdd_pwd.flatMap( lambda reg: re.split("\W+",reg) ).map( lambda reg: (reg,1) )

	# suffle and sort --> agrupo UNOS sin repetir clave y ordeno claves
	rdd_par_suffleSort = rdd_par_map.groupByKey().sortByKey()

	# reduce --> sumo los unos	
	rdd_par_reduce = rdd_par_suffleSort.mapValues(sumar)
	for par in rdd_par_reduce.collect():
		print par
	
	# cierro contexto
	sc.stop()	


