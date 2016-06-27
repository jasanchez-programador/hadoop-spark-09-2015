1.- Contar peticiones por usuario
val rdd_reqPerUser = sc.textFile("/loudacre/weblogs/*").map(reg => (reg.split(" ")(2),1)).groupByKey().sortByKey().mapValues(valores => valores.sum)

3.- lista de ips por usuario
val rdd_usuario_ips = sc.textFile("/loudacre/weblogs/*").map(reg => reg.split(" ") ).map(reg => (reg(2),reg(0))).groupByKey().sortByKey()
def lista_ips(valores: Iterable[String]) : String = {
    var lista = ""
    var i = 0
    var numV = valores.size
    for (valor <- valores){
        if ( i < (numV -1) ){
            lista = lista + valor + ","
		}
        else{
            lista = lista + valor
		}
        i += 1
	}
    return lista
}
rdd_usuario_ips.mapValues(lista_ips).collect()

4.- union datos usuario (nombre apelleido) con numero de peticiones: (userid nombre apellido numPeticiones)
val rdd_userData = sc.textFile("/loudacre/accounts/*").map( reg => reg.split(",") ).map( reg => (reg(0),reg(3) + " " + reg(4)) )
rdd_userData.join(rdd_reqPerUser).map(par => par._1 + " " + par._2._1 + " " + par._2._2).collect()
