**Este proyecto tratará sobre el Análisis de Punto de Venta en Tiempo Real.**<br><br>
>*Problemática:*  
La necesidad de datos en tiempo real en el segmento retail y como superar los desafíos del streaming de datos del punto de venta a escala con un datalakehouse.<br><br>
>*Enfoque Técnico:*  
El enfoque data lakehouse permite emplear múltiples modos de transmisión de datos en paralelo: streaming para datos de alta frecuencia e insert-oriented, y procesos por lotes o batch para eventos menos frecuentes y de mayor escala. Esto comunmente es referido como arquitecturas lambda.  
Se utilizará la arquitectura lambda junto con medallion con el patrón de diseño Bronze, Silver y Gold para aterrizar los datos en etapas.<br><br>
![Architecture](images/Architecture.png)<br><br>
Para ilustrar como la arquitectura lakehouse puede ser aplicada a los datos de un Punto de Venta, desarrollamos un workflow de demostracion dentro del cual calculamos un inventario Near Real Time. Para ello imaginamos 2 puntos de venta transmitiendo información relevante al inventario asociado, con ventas, reabastecimiento y perdidas, a través de transacciones de compras en linea y retiros en tiendas como parte de un inventario en streaming y por otro lado, un snapshot de las unidades de productos en piso que son capturadas por el POS y transmitidas por batch (lotes). Estos datos son simulados para un periodo de un mes y se muestran a una velocidad 10x mayor para una mejor visibilidad de los cambios de inventario.<br>
*La demosnstración hace uso de Azure IOT Hubs y Azure Storage para ingestas, pero deberíamos de trabajar similar en AWS o GCP con la apropiada sustitución de tecnologías.*

