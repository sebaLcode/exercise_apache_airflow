import os
import re
import random
import pandas as pd

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Ubicación del archivo CSV
CSV_FILE_PATH = "/opt/airflow/dags/data/personas.csv"

# Configuración del DAG
TAGS = ['PythonDataFlow', 'MySQL']
DAG_ID = "dag_personas_mysql"
DAG_DESCRIPTION = "Genera CSV sucio, limpia datos y carga información a MySQL"
DAG_SCHEDULE = "0 9 * * *"

default_args = {
    "start_date": datetime(2026, 4, 26),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
#--------------------------

"""
    Genera rut aleatorios con formatos inconsistentes.
    
    Args:
        Ninguno.
        
    Returns:
        str: El rut generado aleatorio.
"""
def generar_rut_sucio():
    #Generando rut aleatorios.
    cuerpo = str(random.randint(1000000, 30000000)) # Genera un números de 7 a 8 dígitos
    dv = random.choice(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9','k', 'K'])
    separador= random.choice(['', '.', '-', ' - '])
    rut = f"{cuerpo}{separador}{dv}"
    
    #Ahora insertamos puntos aleatorios en el rut
    lista_rut = list(rut)
    for _ in range(random.randint(0, 3)):  # Insertar entre 0 y 3 puntos
        pos = random.randint(0, len(lista_rut)-1) # Posición aleatoria para insertar el punto
        lista_rut.insert(pos, '.')
        
    return "".join(lista_rut) 

"""
    Genera fechas aleatorias con formatos inconsistentes.
    
    Args:
        Ninguno.
        
    Returns:
        str: La fecha generada aleatoriamente.
"""
def generar_fecha_sucia():
    dia = str(random.randint(1, 28)).zfill(2)
    mes = str(random.randint(1, 12)).zfill(2)
    anio = "2023"
    
    # Formatos inconsistentes: 2023.01.10, 10-01-2023, 10/01/23, etc.
    formatos = [
        f"{anio}.{mes}.{dia}",
        f"{dia}-{mes}-{anio}",
        f"{dia}/{mes}/{anio[2:]}",
        f"{anio}{mes}{dia}", # Formato pegado
        "fecha_invalida"
    ]
    return random.choice(formatos)

"""
    Genera categorías aleatorias con formatos inconsistentes.
    
    Args:
        Ninguno.
        
    Returns:
        str: La categoría generada aleatoriamente.
"""
def generar_categoria_sucia():
    opciones = [
        "VIP", "vip", "V.I.P.", "  VIP  ", 
        "Standard", "standar", "Estándar", "Stndrd",
        "Premium", "PREMIUM", "premium ", "Premuim" # Error de dedo incluido
    ]
    # También añadimos la posibilidad de que el dato falte (NaN)
    if random.random() < 0.25: # 25% de probabilidad de que sea nulo
        return None
        
    return random.choice(opciones)


"""
    Creamos un task para generar datos CSV sucios.
    
    Args:
        n (int): El número de filas a generar.
        
    Returns:
        None
"""
def generate_csv_data(n=20):
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
    data = []
    for i in range(n):
        fila = {
            "rut": generar_rut_sucio(),
            "fecha_registro": generar_fecha_sucia(),
            "categoria": generar_categoria_sucia()
        }
        data.append(fila)
    # Convertimos a DF y exportamos a CSV
    df = pd.DataFrame(data)
    df.to_csv(CSV_FILE_PATH, index=False)
    print("Archivo CSV generado con datos sucios en:", CSV_FILE_PATH)
    print(df.head())


"""
    Leemos el CSV generado y mostramos los datos sucios.
    
    Args:
        **context: Argumentos de contexto de Airflow.
        
    Returns:
        None
"""
def extract_data(**context):
    df = pd.read_csv(CSV_FILE_PATH)

    print("Datos extraídos del CSV:")
    print(df.head())

    context["ti"].xcom_push(
        key="datos_extraidos",
        value=df.to_json(orient="records")
    )    


"""
    A partir del rut sucio, se limpia para que quede en un formato sin puntos y con guión.
    
    Args:
        rut (str): El rut sucio a limpiar.
        
    Returns:
        str: rut limpio o None si es inválido.
"""
def limpiar_rut(rut):
    if pd.isna(rut): #Si el rut es nulo, devolvemos None
        return None
    
    rut = str(rut).upper() #Convertimos a mayús
    rut = re.sub(r"[^0-9K]", "", rut) # Eliminamos todo excepto números y 'K'
    
    if len(rut) < 8: # Si el rut es demasiado corto, lo consideramos inválido
        return None
    
    cuerpo = rut[:-1]
    dv = rut[-1]
    return f"{cuerpo}-{dv}"
    
"""
    A partir de una fecha sucia, se limpia para que quede en un formato válido.
    
    Args:
        fecha (str): La fecha sucia a limpiar.
        
    Returns:
        str: fecha válida o None si es inválida.
"""
def limpiar_fecha(fecha):
    if pd.isna(fecha):
        return None

    fecha = str(fecha).strip()

    formatos = [
        "%Y.%m.%d",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%Y%m%d",
    ]

    for formato in formatos:
        try:
            return datetime.strptime(fecha, formato).date()
        except ValueError:
            continue

    return None

"""
    A partir de una categoría sucia, se limpia para que quede en un formato válido.
    
    Args:
        categoria (str): La categoría sucia a limpiar.
        
    Returns:
        str: categoría válida o None si es inválida.
"""
def limpiar_categoria(categoria):
    if pd.isna(categoria):
        return None
    
    categoria = str(categoria).strip().upper()
    
    if "VI" in categoria:
        return "VIP"
    elif "ST" in categoria or "EST" in categoria:
        return "Standard"
    elif "PR" in categoria:
        return "Premium"
    else:
        return None

"""
    Se realiza la transformación de los datos.
    
    Args:
        **context: Argumentos de contexto de Airflow.
        
    Returns:
        None
"""
def transform_data(**context):
    datos_json = context["ti"].xcom_pull(
        key="datos_extraidos",
        task_ids="extract_data"
    )

    df = pd.read_json(datos_json, orient="records")

    df["rut"] = df["rut"].apply(limpiar_rut)
    df["fecha_registro"] = df["fecha_registro"].apply(limpiar_fecha)
    df["categoria"] = df["categoria"].apply(limpiar_categoria)

    df = df.dropna(subset=["rut", "fecha_registro"])

    print("Datos transformados:")
    print(df.head())

    context["ti"].xcom_push(
        key="datos_transformados",
        value=df.to_json(orient="records", date_format="iso")
    )


"""
    Se carga la información limpia a una tabla MySQL.
    
    Args:
        **context: Argumentos de contexto de Airflow.
        
    Returns:
        None
"""
def load_data_mysql(**context):
    datos_json = context["ti"].xcom_pull(
        key="datos_transformados",
        task_ids="transform_data"
    )

    df = pd.read_json(datos_json, orient="records")

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS airflow_demo")
    cursor.execute("USE airflow_demo")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS personas (
            rut VARCHAR(20),
            fecha_registro DATE,
            categoria VARCHAR(50)
        )
    """)

    insert_query = """
        INSERT INTO personas (rut, fecha_registro, categoria)
        VALUES (%s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(
            insert_query,
            (
                row["rut"],
                row["fecha_registro"].date()
                if hasattr(row["fecha_registro"], "date")
                else row["fecha_registro"],
                row["categoria"],
            )
        )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Se cargaron {len(df)} registros en MySQL.")

# Definición del DAG
with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    schedule=DAG_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    tags=["PythonDataFlow", "ETL", "MySQL"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    generate_task = PythonOperator(
        task_id="generate_csv_data",
        python_callable=generate_csv_data,
    )

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data_mysql",
        python_callable=load_data_mysql,
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> generate_task >> extract_task >> transform_task >> load_task >> end_task