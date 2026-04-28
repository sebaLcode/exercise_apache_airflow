# Proyecto ETL con Apache Airflow

## Descripción

En este proyecto implementa un pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow.

El flujo consiste en:

1. Generar datos sucios en un archivo CSV.
2. Extraer los datos.
3. Transformar (limpiar) la información.
4. Cargar los datos en una base de datos MySQL.

---

## Tecnologías utilizadas
Se utilizaron las siguientes tecnologías.
- Apache Airflow
- Docker / Docker Compose
- Python (Pandas)
- MySQL

---

## Estructura del proyecto
proyecto-airflow/

├── dags/ ejercicio_DAG.py

├── docker-compose.yml

├── README.md

└── .gitignore

## Flujo del DAG
start → generate_csv_data → extract_data → transform_data → load_data_mysql → end
- generate_csv_data: genera datos con errores y los exporta a .csv.
- extract_data: lee el archivo .csv.
- transform_data: limpia el rut, fecha y categoría.
- load_data_mysql: carga los datos en MySQL.


## Cómo ejecutar el proyecto
A continuación se detallará el paso a paso para ejecutar el proyecto.
1. Clonar el repositorio y entrar a la carpeta.
2. Dentro de la carpeta inicializar Airflow con docker en PowerShell.
   `docker compose up airflow-init`
3. Levantar el entorno.
   `docker compose up`
4. Una vez levantado el entorno, en el navegador acceder a localhost:8080.
5. Ingresar las credenciales.
    usuario: `airflow`
    contraseña: `airflow`
6. Configurar conexión MySQL, en Administradores > Conexiones > Agregar conexión.
   
   `Connection Id: mysql_default`
   
   `Connection Type: MySQL`
   
    `Host: mysql`
   
    `Schema: airflow_demo`
   
    `Login: root`
   
    `Password: root`
   
    `Port: 3306`
8. Finalmente, ejecutar el DAG en Dags > dag_personas_mysql, click en switch y presionar Trigger.
   
Una vez ejecutados los pasos anteriores, se pueden ver los resultados del ETL en MySQL, por lo que dentro del contenedor (carpeta airflow-docker) ejecutar:
    `docker exec -it mysql mysql -u root -p`
Luego en contraseña escribir `root`, una vez dentro ejecutar:
```mysql
USE airflow_demo;
SELECT * FROM personas;
