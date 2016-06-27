# pasamos ficheros passwd1 passwd2 passwd3 que son copias del passwd
# devolvemos el numero de usuarios que tienen una shell valida
# 
# como son 3 ficheros, hay 3 particiones
# la salida la queremos devolver por particion no global

# funcion que devulve id de la Particion y contador de usuarios con shell para esa particion 
def contarBash(idPart, rdd):
     contadorShell = 0
     shell = ""
     for usuario in rdd:
         if usuario.endswith("bash"):
	     shell = usuario.split(":")[6]
             contadorShell += 1
     yield(idPart,(shell,contadorShell))

# cargamos ficheros y trabajamos sobre cada particion de forma independiente(.mapPartitionsWithIndex) 
rddShell = sc.textFile("file:/home/training/julio/Mod14-Particiones/Ficheros/passwd*").mapPartitionsWithIndex(contarBash)

for registro in rddShellCont.collect():
     print registro

