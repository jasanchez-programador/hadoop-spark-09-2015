import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Cambiar mi programa por el nombre de tu aplicacion
object MiPrograma {
	
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
		// parar contexto
		sc.stop()
	}
}
