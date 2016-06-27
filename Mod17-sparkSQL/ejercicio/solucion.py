**** datos de web page: ****
$ hdfs dfs -cat /loudacre/webpage/part-m-00000 | head -n 5
schema: id	pagina	ficheros_asociados
1	sorrento_f00l_sales.html	theme.css,code.js,sorrento_f00l.jpg
2	titanic_2100_sales.html	theme.css,code.js,titanic_2100.jpg
3	meetoo_3.0_sales.html	theme.css,code.js,meetoo_3.0.jpg
4	meetoo_3.1_sales.html	theme.css,code.js,meetoo_3.1.jpg
5	ifruit_1_sales.html	theme.css,code.js,ifruit_1.jpg

****  datos salida en JSON /loudacre/webpage_files_json : ****
schema: web_page_num, associated_file
1	theme.css
1	code.js
1	sorrento_f00l.jpg


# 1. Cargar datos en rdd de /loudacre/webpage
rdd_webpage = sc.textFile("/loudacre/webpage/*")
rdd_webpage_array = rdd_webpage.map( lambda reg: reg.split('\t') )

# 2. Pasar de rdd a DataFrame(df)
df_webpage = sqlctx.createDataFrame(rdd_webpage_array,['id','pagina','ficheros_asociados'])

df_webpage.printSchema()
root
 |-- id: string (nullable = true)
 |-- pagina: string (nullable = true)
 |-- ficheros_asociados: string (nullable = true)

df_webpage.show(2)
id pagina               ficheros_asociados  
1  sorrento_f00l_sal... theme.css,code.js...
2  titanic_2100_sale... theme.css,code.js...

# 3. quedarnos con id y ficehros_asociados
df_webpage_final = df_webpage.select("id","ficheros_asociados")

# 4. pasar de df a rdd
rdd_webpage_final = df_webpage_final.rdd

# 5. crear parRDD clave:id / valor:ficheros_asociados 
par_rdd_webpage_final = rdd_webpage_final.map(lambda row: (row.id,row.ficheros_asociados) )

# 6. crear parRDD con clave:id / valor:fichero_asociado
par_rdd_webpage_final2 = par_rdd_webpage_final.flatMapValues(lambda valores: valores.split(','))

# 7. Pasar del parRDD a df
df_webpage_final2 = sqlctx.createDataFrame(par_rdd_webpage_final2,['idPagina','ficheroAsociado'])

# 8. Guardar formato json en /loudacre/webpage_files_json
df_webpage_final2.save('/loudacre/webpage_files_json','json')


