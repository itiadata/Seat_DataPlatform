from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from dotenv import load_dotenv
import pandas as pd
import os
import unicodedata
import xmltodict
import json
import glob


# Variables locales
dbt_project_path = "/usr/local/airflow/dags/"
load_dotenv()
account_url = os.getenv("account_url")
sas_token = os.getenv("sas_token")
file_path = (
    dbt_project_path + "descargas_csv"
)  # Ruta local donde se guardará el archivo


def obtain_name(path):
    file_name = os.path.basename(path)
    # Obtener la ruta del directorio
    # Separar el nombre del archivo y la extensión
    file_name_only, file_extension = os.path.splitext(file_name)
    return file_name_only


def change_format(download_file_path, blob_name):
    new_name = os.path.splitext(blob_name)[0]
    path_xlsx = download_file_path + "/" + blob_name
    if blob_name.endswith(".xlsx"):
        new_name = os.path.splitext(blob_name)[0]
        # Lee el archivo Excel
        df = pd.read_excel(path_xlsx)
        # Especifica la ruta del archivo CSV de salida
        csv_file = download_file_path + "/" + new_name + ".csv"
        # Escribe el DataFrame en un archivo CSV
        df.to_csv(csv_file, index=False)
        print(f"El archivo {file_path} ha sido convertido a {csv_file}.")
        delete_file(path_xlsx)
        return new_name + ".csv"
    elif blob_name.endswith(".xml"):
        # Especifica la ruta del archivo JSON de salida
        json_file = download_file_path + "/" + new_name + ".json"
        # Lee el archivo XML
        with open(download_file_path + "/" + blob_name, "r") as file:
            xml_content = file.read()
        # Convierte el XML a un diccionario de Python
        data_dict = xmltodict.parse(xml_content)
        # Convierte el diccionario a un JSON
        json_data = json.dumps(
            data_dict, indent=4
        )  # indent=4 para un JSON bien formateado
        # Guarda el JSON en un archivo
        with open(json_file, "w") as file:
            file.write(json_data)
        print(f"El archivo {file_path} ha sido convertido a {json_file}.")

    else:
        return blob_name


def delete_file(file_path):
    # Verifica si el archivo existe antes de intentar eliminarlo
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"El archivo {file_path} ha sido eliminado.")
    else:
        print(f"El archivo {file_path} no existe.")


def snowflake_con():
    # Create the SQLAlchemy engine
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_id1")
    engine = dwh_hook.get_cursor()

    with dwh_hook.get_conn() as connection:
        with connection.cursor() as cursor:
            # Ejecuta la consulta
            cursor.execute("SELECT CURRENT_VERSION()")
        # Obtén los resultados
        rows = cursor.fetchall()
        # Procesa los resultados (por ejemplo, imprime el primer resultado)
        if rows:
            print(rows[0][0])
        else:
            print("No se encontraron resultados.")
    return engine


def remove_accents(input_str):
    """Elimina los acentos de una cadena de texto."""
    # Normaliza la cadena para descomponer los caracteres acentuados en su forma base y los acentos
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    # Filtra solo los caracteres que no son marcas diacríticas
    return "".join(c for c in nfkd_form if not unicodedata.combining(c))


# Detectar automaticamente el delimitador del csv
def detect_delimiter(file_path):
    with open(file_path, "r") as file:
        first_line = file.readline()
        # Asume que los delimitadores posibles son ',', ';', '\t'
        if "," in first_line:
            return ","
        elif ";" in first_line:
            return ";"
        elif "\t" in first_line:
            return "\t"
        else:
            raise ValueError("Delimiter not found")


# executa las queries que estan en rvscode.sql
def execute_query_by_name(query_name, params, conn):
    # Lee el archivo SQL
    file_name = dbt_project_path + "csvtosnowflake.sql"
    if params is None:
        params = {}

    with open(file_name, "r") as file:
        contenido = file.read()
        consultas = contenido.split("--")
        consulta_sql = ""
        for consulta in consultas:
            if consulta.strip().startswith(f"@{query_name}"):
                consulta_sql = consulta.strip().split("\n", 1)[1].strip()
                break
        else:
            raise ValueError("No se encontró la consulta especificada en el archivo")
    # executa la query que se manda
    consulta_formateada = consulta_sql.format(**params)
    print(consulta_formateada)
    result = conn.execute(consulta_formateada)
    rows = result.fetchall()
    print((rows[0][0]))
    return rows[0][0]


# Descarga el csv y lo inserta a snowflake en forma de tabla
def download_blob_to_file(conn):
    for file in glob.glob(os.path.join(file_path, "*.csv")):
        print(file)
        # Si un fichero viene mal formateado le hacemos un arreo
        name_table = obtain_name(file)
        name_file_stage = name_table + ".csv"
        delimiter = detect_delimiter(file)
        path_route = file
        parametros = {
            "path_route": path_route,
            "stage_name": "UC_FSN_MARC_TEST.FILE_CSV",
            "name_csv": file,
            "name_csv_stage": name_file_stage,
            "name_table": name_table,
            "delimiter": delimiter,
            "schema": "UC_FSN_MARC_TEST",
            "database": "sandbox",
        }
        execute_query_by_name("databasedefintion", None, conn)
        execute_query_by_name("userwarehouse", None, conn)
        execute_query_by_name("fileformat_csv", parametros, conn)
        execute_query_by_name("createstage", parametros, conn)
        execute_query_by_name("addfilestage", parametros, conn)
        execute_query_by_name("createtable", parametros, conn)
        execute_query_by_name("copyinto", parametros, conn)
        # Elimina el fichero del blob


# función principal para que funcione todo el proceso
def createstage():
    conn = snowflake_con()
    download_blob_to_file(conn)
