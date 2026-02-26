**Resolución Transitiva de Claves 1:N**<br><br>
>*Problemática:<br>

Nos encontramos ante un escenario de resolución de claves y granularidad de información:<br>
- Al utilizar dos sistemas distintos las claves en uno y otro no son las mismas.<br>
- Se necesita de una tabla transitiva para poder mapear las claves entre sistemas.<br>
- Al hacer este "de-para" con la tabla transitiva las claves se duplican 1:N afectando la granularidad.<br>
- Al realizar cruces directos, utilizando la tabla transitiva, duplicamos información.<br><br>

Sistema A -> ventas <br>
Sistema B -> forecast <br>
Tabla Transitiva -> productos <br>
Problema 1:N <br>
Resolución determinista. <br><br>

>*Enfoque Técnico: <br>

Patrón: Agrupar -> Expandir -> Resolver -> Re-Agrupar <br>
- collect_list(): Agrupa todas las claves candidatas en una colección por clave principal. <br>
- explode(): Se expanden las opciones para analizarlas individualmente. <br>
- row_number(): Window + criterio deterministico. <br>
- group_by(): Finalmente restuaramos la granularidad. <br><br>

>*Objetivo:<br>

Evaluar todas las opciones posibles bajo control y seleccionar una sola forma determinística.<br><br>

>*Aprendizaje:<br>

Con esto logramos una estrategia de resolución determinística de claves ambiguas en relaciones 1:N mediante control explícito de granularidad y evaluación exhaustiva de candidatos.<br><br>

>*¿Qué resolvimos? <br>

- La apertura 1:N <br>
- La ambigüedad del mapeo <br>
- Sin duplicar métricas <br>
- Conservando la granularidad original<br>
