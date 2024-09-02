from minio import Minio
from minio.error import S3Error
import urllib3
from urllib3 import Retry
import socket
import pandas as pd
import glob, os
from dotenv import load_dotenv
import snowflake.connector
from sqlalchemy.orm import sessionmaker
import shutil
from datetime import date
from pathlib import Path
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
from time import sleep
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

import requests


# Desactivar las advertencias de verificación de SSL
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


dbt_project_path = ("/usr/local/airflow/dags/")

def delete_folder(schema):
    if os.path.isdir(dbt_project_path+schema):
        shutil.rmtree(dbt_project_path+schema)
    elif os.path.isfile(dbt_project_path+schema):
        os.remove(dbt_project_path+schema)
    else:
        print(f"The path {dbt_project_path+schema} does not exist.")
     

def devolver_year(name):
    partes = name.split('/')
    # Obtener la parte que contiene el año (DAPC_YEAR=2024)
    parte_con_year = partes[2]

    # Dividir esa parte en función del signo igual (=)
    valor_year = parte_con_year.split('=')

    # Obtener el año y convertirlo a entero
    year_string = valor_year[1]
    year_obj=0

    # Verificar si el año contiene solo dígitos
    if year_string.isdigit():
        year_obj = int(year_string)
        
        print("Año:", year_obj)
    else:
        print("El valor del año no es un número válido.")
    return year_obj

def devolver_mes(name):
    partes = name.split('/')
    # Obtener la parte que contiene el año (DAPC_YEAR=2024)
    parte_con_mes = partes[3]

    # Dividir esa parte en función del signo igual (=)
    valor_mes = parte_con_mes.split('=')

    # Obtener el año y convertirlo a entero
    mes_string = valor_mes[1]
    mes_obj=0

    # Verificar si el año contiene solo dígitos
    if mes_string.isdigit():
        mes_obj = int(mes_string)
        
        print("Mes:", mes_obj)
    else:
        print("El valor del año no es un número válido.")
    return mes_obj



def snowflake_con(schema):
   
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
    
   

def execute_query_by_name(query_name, params,conn):
    # Lee el archivo SQL
    file_name = dbt_project_path+'minicode.sql'
    print(file_name)

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


hostname = 'storage.esqa.dapc.ocp.vwgroup.com'


def insert_to_sourcetable(name,conn):
    partes = name.split('/')
    table = partes[1]
    schema = partes[0]
    name_parquet = partes[4]
    fecha = date.today()
    add = {'table_name': table, 'fecha': fecha, 'name_file': name, 'schema':schema}
    execute_query_by_name('inserdata',add,conn)

def insert_to_snowflake(tabla,file_path, schema ,engine):
        partes = file_path.split('/')
        name_parquet = partes[9]
        print(name_parquet)
        database='DEV_STAGING_DB'
        stage = f"{database}.{schema}.{tabla}"

        arguments = {'stage_name':stage, 'schema': schema, 'nametable': tabla,'name':file_path,'name_parquet':name_parquet}
        
        val= execute_query_by_name('existstage',arguments,engine)
        if int(val) == 0 :
            execute_query_by_name('createstage',arguments,engine)
        execute_query_by_name('addfilestage',arguments,engine)
        #execute_query_by_name('createformat',arguments,engine)
        execute_query_by_name('createtable',arguments,engine)
        execute_query_by_name('copytotable',arguments,engine)
        #execute_query_by_name('removestage',arguments,engine)

def processwritesnowflake(object_name,client,bucket_name,engine,tabla):
    file_path = dbt_project_path+object_name  # Ruta local donde se guardará el archivo
    client.fget_object(bucket_name, object_name, file_path)
    print(f"Archivo descargado: {object_name}")
    #transformar parquet a dataframe
    #inserta source table control
    #insertar datos a snowflake
    partes = object_name.split('/')
    insert_to_snowflake(tabla,file_path, partes[0] ,engine)
    insert_to_sourcetable(object_name,engine)
    #eliminar archivo
    sleep(10)
    if os.path.exists(dbt_project_path+object_name):
        os.remove(dbt_project_path+object_name)
    else:
        print(f"File or directory {dbt_project_path+object_name} does not exist.")
    
def func(schema,tabla,year,month):

   #procerso principal para la transformaciond de parquet a pandas para escribir hacía snowflake

    #conexion con minIO
    try:
        ip_address = socket.gethostbyname(hostname)
        print(f'The IP address   {hostname} is {ip_address}')
    except socket.gaierror as e:
        print(f'Error: {e.strerror}')
        

    httpclient = urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=10.0, read=10.0),
        cert_reqs = 'CERT_NONE',
        maxsize = 5,
        retries= Retry(
                    total=5,
                    backoff_factor=0.2,
                    status_forcelist=[500, 502, 503, 504]
                )
    )
    '''httpclient2 = urllib3.ProxyManager(
                    'https://proxy_host.sampledomain.com:8119/',
                    cert_reqs='CERT_NONE',
                    ca_certs='CA-Bundle.crt',
                    timeout=urllib3.Timeout(connect=10.0, read=10.0)
                )'''

    client = Minio("storage.esqa.dapc.ocp.vwgroup.com", 
        access_key="EsqaS3OZCSEAT",
        secret_key="bxe3ymy_pwq1CHP7fbm",
        secure=True,
        http_client=httpclient
    )


    bucket_name = 'sqa-sz-storage'

    prefix = schema
    recursive = True
    listaobjet=[]
    sleep(5)
    #procese de descarga de los parquet y su tratamiento
    try:
        objects = client.list_objects(bucket_name, prefix=prefix+'/'+tabla+'/DAPC_YEAR='+str(year)+'/DAPC_MONTH='+str(month), recursive=recursive)
        
        engine = snowflake_con('your_schema')
 
        filestart = schema+'/'+tabla

        parametros = {'schema' : schema, 'name':''}
        execute_query_by_name('defaultdatabase', parametros,engine)
        execute_query_by_name('createschema', parametros,engine)
        execute_query_by_name('createcontrol', parametros,engine)
        execute_query_by_name('createformat', parametros,engine)


        if  int(execute_query_by_name('firstload', parametros,engine)) == 0 :
            for obj in objects:
                name =obj.object_name
                print(name)
                if (obj.object_name).endswith(".parquet") and     (obj.object_name).startswith(filestart+'/DAPC_YEAR')  and not (obj.object_name).endswith("checkpoint.parquet") and devolver_year(name)==year and devolver_mes(name)==month :
                    object_name = obj.object_name
                    processwritesnowflake(object_name,client,bucket_name,engine,tabla)
        else:
            for obj in objects:
                name =obj.object_name
                print(name)
                if (obj.object_name).endswith(".parquet") and  (obj.object_name).startswith(filestart+'/DAPC_YEAR')  and not (obj.object_name).endswith("checkpoint.parquet") and devolver_year(name)==year and devolver_mes(name)==month :
                    parametros = {'name': obj.object_name, 'schema': schema}
                    val=execute_query_by_name('existeparquet', parametros,engine)
                    print(val)
                    if (int(val) == 0):
                        object_name = obj.object_name
                        processwritesnowflake(object_name,client,bucket_name,engine,tabla)
                       
                    else :
                        continue
        engine.close()
       
    except S3Error as e:
        print(f'Error listing objects: {e}')



'''
CARPORT
DISS
DIAGNOSE
SAGA
'''    


