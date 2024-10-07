from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import pandas as pd
import shutil
import os
import re
import unicodedata
import xmltodict
import json
import pandas as pd



# Variables locales 
dbt_project_path = ("/usr/local/airflow/dags/")
load_dotenv()
account_url = os.getenv("account_url")
sas_token= os.getenv("sas_token")
file_path = dbt_project_path+"descargas_csv"  # Ruta local donde se guardará el archivo

def change_format(download_file_path,blob_name):

    new_name = os.path.splitext(blob_name)[0]
    path_xlsx=download_file_path+'/'+blob_name
    if (blob_name.endswith('.xlsx')):
        new_name = os.path.splitext(blob_name)[0]
        # Lee el archivo Excel
        df = pd.read_excel(path_xlsx)
        # Especifica la ruta del archivo CSV de salida
        csv_file = download_file_path+'/'+new_name+'.csv'
        # Escribe el DataFrame en un archivo CSV
        df.to_csv(csv_file, index=False)
        print(f"El archivo {file_path} ha sido convertido a {csv_file}.")
        delete_file(path_xlsx)
        return new_name+'.csv'
    elif blob_name.endswith('.xml'):

        # Especifica la ruta del archivo JSON de salida
        json_file = download_file_path+'/'+new_name+'.json'
        # Lee el archivo XML
        with open(download_file_path+'/'+blob_name, 'r') as file:
            xml_content = file.read()
        # Convierte el XML a un diccionario de Python
        data_dict = xmltodict.parse(xml_content)
        # Convierte el diccionario a un JSON
        json_data = json.dumps(data_dict, indent=4)  # indent=4 para un JSON bien formateado
        # Guarda el JSON en un archivo
        with open(json_file, 'w') as file:
            file.write(json_data)
        print(f"El archivo {file_path} ha sido convertido a {json_file}.")

    
    else :
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
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_id")
    engine= dwh_hook.get_cursor()
    
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
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    # Filtra solo los caracteres que no son marcas diacríticas
    return ''.join(c for c in nfkd_form if not unicodedata.combining(c))

#Detectar automaticamente el delimitador del csv 
def detect_delimiter(file_path):

    with open(file_path, 'r') as file:
        first_line = file.readline()
        # Asume que los delimitadores posibles son ',', ';', '\t'
        if ',' in first_line:
            return ','
        elif ';' in first_line:
            return ';'
        elif '\t' in first_line:
            return '\t'
        else:
            raise ValueError("Delimiter not found")

#executa las queries que estan en rvscode.sql
def execute_query_by_name(query_name, params ,conn):
    # Lee el archivo SQL
    file_name = dbt_project_path+'rvscode.sql'
    if params is None:
        params = {}

    with open(file_name, 'r') as file:
        contenido = file.read()
        consultas = contenido.split('--')
        consulta_sql = ''
        for consulta in consultas:
            if consulta.strip().startswith(f"@{query_name}"):
                consulta_sql = consulta.strip().split("\n", 1)[1].strip()
                break
        else:
            raise ValueError("No se encontró la consulta especificada en el archivo")
    #executa la query que se manda
    consulta_formateada = consulta_sql.format(**params)
    print(consulta_formateada)
    result=conn.execute(consulta_formateada)
    rows = result.fetchall()
    print((rows[0][0]))
    return (rows[0][0])


#Descarga el csv y lo inserta a snowflake en forma de tabla 
def download_blob_to_file(conn):
    blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
    container_client = blob_service_client.get_container_client('hvr-prueba-csv')
    print(container_client)
    download_folder=dbt_project_path+'descargas_csv'
    os.makedirs(download_folder, exist_ok=True)
    allowed_extensions = ['.csv', '.xml', '.json','xlsx']


    # Listar y descargar los archivos CSV
    blobs = container_client.list_blobs()
    print(blobs)

    for blob in blobs:
        if any(blob.name.endswith(ext) for ext in allowed_extensions):
            # Crear un BlobClient
           
            blob_client = container_client.get_blob_client(blob)
            print('el nombre del archivo es: '+ blob.name)
            blob_name = remove_accents(blob.name)

            # Ruta completa para guardar el archivo descargado
            download_file_path = os.path.join(download_folder, blob_name)
            
            # Descargar el blob
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

            #Si un fichero viene mal formateado le hacemos un arreo
            name_table = re.sub(r'[^\w]', '_',  os.path.splitext(blob_name)[0])
            print(name_table)

            blob_name=change_format(download_folder,blob_name)
            print(blob_name)

            print(f"Archivo descargado: {download_file_path}")
            delimiter= detect_delimiter(file_path+'/'+blob_name)
            path_route = download_folder+"/"+blob_name
            execute_query_by_name('databasedefintion',None,conn)
            execute_query_by_name('createstage',None,conn)
            parametros = {'path_route' : path_route , 'stage_name':'rvs_table.RVS_FILE_CSV','name_csv':blob_name,'name_table': name_table,'delimiter': delimiter}
            execute_query_by_name('addfilestage',parametros,conn)
            execute_query_by_name('fileformat_csv',parametros,conn)
            execute_query_by_name('createtable',parametros,conn)
            execute_query_by_name('copyinto',parametros,conn)
            #Elimina el fichero del blob
            blob_client.delete_blob()


#función principal para que funcione todo el proceso
def createstage():
    conn = snowflake_con()
    download_blob_to_file(conn)

#Funcion para hacer el delete del folder , de esa manera no es permanete en el proyecto
def delete_folder():
    folder=dbt_project_path+'descargas_csv'
    if os.path.isdir(folder):
        shutil.rmtree(folder)
    elif os.path.isfile(folder):
        os.remove(folder)
    else:
        print(f"The path {folder} does not exist.")
     

