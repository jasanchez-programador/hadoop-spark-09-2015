# spark-submit --master local fichero.py rutaFichero

import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
	# Si el faltan argumentos a ejecutar SALIR con ERROR
	if len(sys.argv) < 1:
		print >> sys.stderr, "ERROR: faltan argumentos"
		exit(-1)

	sconf = SparkConf()
	sconf.setAppName("Mi plantilla para programa")
	sconf.set("spark.ui.port","4141")

	sc = SparkContext(conf=sconf)

	"""
	aqui ira nuestro programa

	sc.textFile(sys.argv[1])

	"""

	sc.stop()	
