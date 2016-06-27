import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Cambiar mi programa por el nombre de tu aplicacion
object ContarPalabrasPasswd {
	
	def main(args: Array[String]){
		// Si el numero de argumentos no es correcto SALIR con ERROR
		if (args.length < 1){
			System.err.println("ERROR: faltan argumentos")
			System.exit(-1)
		}
		
        val sconf = new SparkConf()
        sconf.setAppName("mi plantilla programa")
        sconf.set("spark.ui.port","4141")

        val sc = new SparkContext(sconf)

/*
		Aqui va nuestro programa
*/
		def sumar(valores: Iterable[Int]): Int = {
			var suma = 0
			for ( valor <- valores){
				suma += valor
			}
			return suma
		}

		// cargamos fichero
		var rdd_pwd = sc.textFile(args(0))

		// Fase Map --> palabra, 1
		var rdd_par_map = rdd_pwd.flatMap( reg => reg.split("\\W+") ).map( reg => (reg,1) )

		// Fase Shuffle and Sort --> agrupamos unos quitando claves repetidad y ordenamos por clave
		var rdd_par_shuffleSort = rdd_par_map.groupByKey().sortByKey()

		// Fase Reduce --> sumamos los unos
		var rdd_par_reduce = rdd_par_shuffleSort.mapValues(sumar)

		rdd_par_reduce.foreach(println)

		// parar contexto
		sc.stop()
	}
}
