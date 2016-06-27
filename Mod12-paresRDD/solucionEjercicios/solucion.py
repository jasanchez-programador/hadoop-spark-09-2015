1.- Contar peticiones por usuario
rdd_reqPerUser = sc.textFile("/loudacre/weblogs/*").map(lambda reg: (reg.split(" ")[2],1)).groupByKey().sortByKey().mapValues(lambda valores: sum(valores))

3.- lista de ips por usuario
rdd_usuario_ips = sc.textFile("/loudacre/weblogs/*").map(lambda reg: reg.split(" ") ).map(lambda reg: (reg[2],reg[0])).groupByKey().sortByKey()
def lista_ips(valores):
    lista = "";
    i=0
    numV = len(valores)
    for valor in valores:
		if ( i < (numV -1) ):
        	lista = lista + valor + ","
		else:
    		lista = lista + valor
        i += 1
    return lista
rdd_usuario_ips.mapValues(lista_ips).collect()

4.- union datos usuario (nombre apelleido) con numero de peticiones: (userid nombre apellido numPeticiones)
rdd_userData.join(rdd_reqPerUser).map(lambda (clave,(valorClave,valor)): clave + " " + valorClave + " " + str(valor)).collect()
