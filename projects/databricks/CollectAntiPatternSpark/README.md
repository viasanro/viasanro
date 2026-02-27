**collect() - Anti Patrón en Spark**<br><br>
>Problemática:<br>

El escenario es el siguiente, se necesita realizar una transformación de datos iterando sobre los registros de un dataframe de spark;<br>
como la transformación es compleja (en el escenario laboral real) se realizaron algunos ajustes para simplificar el proceso,<br>
esto derivo en lo siguiente:<br>
- Se intentó solucionar con broadcast join. No redujo la complejidad ni los tiempos.<br>
- Se intentó implementar repartition. Tampoco redujo la complejidad ni los tiempos.<br>
- Necesariamente se tuvo que hacer un collect().<br> Con esto se redujo la complejidad, pero no los timpos.<br>
collect() es un antipatrón de Spark ya que no trabaja de forma distribuida y obliga a mover todos los datos de dicho dataframe al Driver,<br>
esto no afecta si el dataset es pequeño, pero el problema real está cuando utilizamos este dataframe para cruzarlo con otro.<br>
Por ejemplo en nuestro caso si hacemos algo como:<br><br>
>for row in small_data:<br> &nbsp;&nbsp;&nbsp;&nbsp; df_big = df_big.filter(df_big.descripcion == row.descripcion)<br>

Aquí pasa lo siguiente:<br>
- El loop es en el driver.<br>
- Cada iteración crea una transformación nueva.<br>
- Spark sigue distribuido, pero estás controlando la lógica desde el Driver.<br>
- Se vuelve ineficiente y puede generar muchos stages.<br><br>

>Solución Propuesta:<br>

Como nos dimos cuenta que usar collect era lo único que reducía nuestra complejidad en nuestro caso, ahora tocaba mejorar el performance.<br>
La solución fué utilizar **Chunked Join** para procesar por lotes (chunks) en lugar de iterar 1 a 1 por las filas del dataframe.<br><br>

>Aprendizaje:<br>

Muchas veces en la documentaión de Spark nos dicen: "Nunca uses collect()". Eso es cierto si el datset es gigante porque matarás la memoria del Driver y la red, pero si tienes un dataset pequeño traerlo al Driver es una estratégia de optimización válida.<br>
Por otro lado, el chunked join podría ser una solución válida cuando:<br>
- El join explota en memoria.<br>
- Hay skew fuerte.<br>
- El shuffle es demasiado grande.<br>
- El cluster es pequeño.<br>
- El driver se satura.<br>
- Broadcast join no resuelve tu problema.<br><br>

>¿Qué resolvimos? <br>

En mi caso particular, me sirvió para mejorar los tiempos de ejecución de un DAG (en el escenario laboral real) que tardaba 3.5 horas en promedio, paso a demorar 30 min en promedio.
