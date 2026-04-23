# Ejercicio 1 – Orquestación con Airflow y MinIO

Este ejercicio implementa una arquitectura básica de ingesta y procesamiento de datos utilizando Airflow como orquestador y MinIO como almacenamiento de objetos, todo desplegado mediante Docker Compose.

## Contenido

- **docker-compose.yaml**: Orquestación de servicios
- **data_prueba_tecnica.csv**: Dataset de entrada
- **Airflow/**: Configuración y definición de DAGs
- **Minio/**: Configuración del almacenamiento
- **Trino/**: Motor de consultas

## Preguntas adicionales

1. Para los ids nulos ¿Qué sugieres hacer con ellos ? 
   De primera mano esto rompe la unicidad del registro y la trazabilidad, de igual manera si esto no se trata dentro del ETL podria causar duplicidad, por base teorica el id tendria que ser not null y unique, dentro del ETL en el tratado de los datos se podrian excluir y enviar estos registros a una tabla de errores para poder hacer un analisis.
2. Considerando las columnas name y company_id ¿Qué inconsistencias notas 
y como las mitigas? 
   Pues en la mayoria de los registros su relacion es consistente MiPasajefy - cbf1c8b09cd5b549416d49d220a40cbd317f952e y Muebles chidos - 8f642dc67fccf861548dfe1c761ce22f795e91f0 pero tambien hay casos donde se rompe, es decir company_id nulo con name presente o name nulo con company_id presente y algunos company_id con valores raros ****

   Para solucionar todo esto podriamos crear una dimension de compañias con las compañias activas y validas con una combinacion name-company_id consistente y para los nulos un valor comodin -99 asegurando integridad referencial.

3. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo 
procedes?  

   Si, en amount el valor 3.0 aparece de manera masiva para voided y pending_payment lo cual es sospechoso, podrian ser registros de prueba o simplemente error de origen, lo que podriamos hacer es validar con negocio que esto sea correcto o es data defectuosa y si lo es poner limites razonables para mitigar este error.

4. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones? 

   Me tome la libertad de en la tarea 1 validar que el bucket de landing y archivo fuente exista, si este no existe el flujo se detiene y marca el error Archivo requerido no encontrado en MinIO de igual forma se valida que el bucket destino exista si este no existe se crea ya que es donde llegara nuestra info en bronze.

   Otra mejora a simple vista es generar otro bucket de estilo archive para despues de procesado el archivo fuente moverlo a este bucket dentro de una carpeta con un timestamp para guardar ese historico de fuentes.

   
5. Guardar una captura de pantalla como imagen, de la query con Trino usando 
DBeaver