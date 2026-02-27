## Asunciones
**Datasets**<br>
- Por cuestiones de simplificación, se generaron 2 datsets, similares en estructura pero identicos en valores.<br>
- La función de transformación se simplifica con fines didácticos.<br><br>

## Como ejecutarlo en Databricks<br>
1. Abre tu Workspace en Databricks<br>
2. Click en Importar<br> 
3. Subir archivo 'chunked-query-optmization-nb.py'<br>
4. Click en Data Ingestion<br>
5. Aqui tienes la opcion de crear una tabla o subirlo directamente a un volumen. (los archivos a ingestar se encuentran en la carpeta data)<br>
 Yo, particularmente los subí a un volumen, en tu caso deberias de subir los archivos donde más te convenga, luego modficas las celdas 3 y 4 de la notebook respectivamente que es donde ser realiza la carga de datos.<br>
 *Importante:* mantener el nombre de los dataframes para que el resto del código funcione.<br>
6. Selecciona un Cluster<br>
7. Ejecuta todas las celdas <br>