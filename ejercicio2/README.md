# Prueba Técnica – Data Engineering

Repositorio que contiene la solución a la prueba técnica solicitada, dividido en dos ejercicios.

## Estructura del repositorio

- **ejercicio1/**  
  Implementación práctica usando Docker Compose con Airflow y MinIO.
  
- **ejercicio2/**  
  Diseño de arquitectura y justificación técnica.

A. Subconjunto de datos a extraer por fuente

F1 (CRM propietario):
- Identificador único de cliente
- Datos demográficos relevantes (edad, país, segmento)
- Datos de contacto necesarios para operación
No se extraen datos históricos irrelevantes ni campos operativos internos del CRM.

F2 (SQL Server – transacciones de productos A):
- ID transacción
- ID cliente
- ID producto
- Fecha de transacción
- Monto y estado

F3 (PostgreSQL – transacciones de productos B):
- Campos equivalentes a F2 para permitir un modelo transaccional unificado

El subconjunto se define con base en requerimientos de negocio y principios de minimización de datos.

B. Retos de extracción y herramientas

Retos:
- Sistemas productivos 24/7
- Cargas incrementales
- Diferentes motores de base de datos
- SCD2 hibridos para dims

Herramientas propuestas:
- Conectores CDC (Change Data Capture) cuando sea posible JDBC Y API
- SQL Server y PostgreSQL
- APIs o dumps controlados para el CRM propietario

C. Retos por independencia de modelos de datos

Retos:
- Diferentes convenciones de nombres
- Tipos de datos distintos
- Llaves de negocio no alineadas
- Análisis con negocio, tiempo 

Mitigación:
- Capa de estandarización semántica
- Catálogo de entidades (Customer, Transaction, Product)
- Transformaciones de mapeo y normalización en capa silver
- Establecer reuniones semanales para evitar stopers

D. Mitigación de impacto sobre fuentes originales

- Uso de CDC en lugar de queries full scan
- Extracciones incrementales basadas en timestamps
- Réplicas de lectura cuando estén disponibles
- Ventanas batch fuera de horas pico

E. Etapas de transformación de datos

Bronze:
- Datos crudos, sin transformación, auditables

Silver:
- Limpieza, estandarización de esquemas
- Resolución de llaves
- Validaciones de calidad
- Enriquecimiento de datos
- Aplicación SCD2 hibrida

Gold:
- Datos agregados y modelados para consumo
- Tablas optimizadas para queries operativas y analíticas

F. Herramientas de transformación

- Apache Spark o Polars para procesamiento batch
- SQL para transformaciones simples
- databricks para reglas de negocio y versionado de modelos
- Todo orquestado por Airflow

G. Storage por propósito

- Data Lake (S3 / MinIO / ADLS): Bronze y Silver (Parquet)
- Lakehouse (Trino / Iceberg / Delta): consultas SQL operativas y  ciencia de datos avanzada 

H. Orquestación del pipeline

- Airflow como orquestador principal
- DAGs con ejecución diaria e incremental
- Sensores y validaciones entre capas


II. SEGURIDAD


- Gestión de secretos con Vault o Secrets Manager
- IAM por roles y principio de menor privilegio
- Segmentación de red y controles de acceso
- Auditoría y logging centralizado

III. GOBERNANZA DE DATOS

- Catálogo de datos centralizado
- Versionado de esquemas y modelos (databricks + Git)
- Monitoreo de calidad de datos
- Documentación como parte del pipeline


## Autor
César Hernández Hernández
``