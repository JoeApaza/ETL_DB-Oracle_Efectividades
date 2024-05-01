import logging
import os
import oracledb
import pandas as pd
from dotenv import load_dotenv
from src.database.db_oracle import close_connection_db,read_database_db,leer_sql,get_connection,ejecutar_consultas,Insert_dataframe_db,read_database_db,IngresarDatos
from src.routes.Rutas import ruta_Pagos_OAC,ruta_Extraer_NroDoc,ruta_Insertar_Data_Tabla,Creacion_tabla,Creacion_Efectividades_Corp
from datetime import datetime, timedelta
 
fecha_actual = datetime.now()
fecha_ayer = fecha_actual - timedelta(days=1)
año = fecha_ayer.strftime('%Y')
mes = fecha_ayer.strftime('%m')
dia = fecha_ayer.strftime('%d')

Actualizar_Pagos_PI = './scripts/Actualizar_Pagos_PI.sql'
Actualizar_Pagos_PII = './scripts/Actualizar_Pagos_PII.sql'
Creacion_Historico_y_Pagos='./scripts/Creacion_Historico_y_Pagos.sql'

logging.basicConfig(format="%(asctime)s::%(levelname)s::%(message)s",   
                    datefmt="%d-%m-%Y %H:%M:%S",    
                    level=10,   
                    filename='.//src//utils//log//app.log',filemode='w')


load_dotenv()
Conexion_OAC = get_connection(os.getenv('USER_DB_1'),os.getenv('PASSWORD_DB_1'),os.getenv('DNS_DB_1'))
Conexion_Opercom=get_connection(os.getenv('USER_DB_2'),os.getenv('PASSWORD_DB_2'),os.getenv('DNS_DB_2'))

origen_cursor = Conexion_OAC.cursor()
destino_cursor = Conexion_Opercom.cursor()

ejecutar_consultas(Actualizar_Pagos_PI,Conexion_Opercom)
 
Query_Extraer_Nro_Doc=leer_sql(ruta_Extraer_NroDoc)
data=destino_cursor.execute(Query_Extraer_Nro_Doc)
data = pd.read_sql(Query_Extraer_Nro_Doc, Conexion_Opercom)
 
 

cadena_Nro_Doc= ','.join("'" + str(valor) + "'" for valor in data['NRO_DOCUMENTO'])

values_Nro_Doc = cadena_Nro_Doc.split(',')
 

group_size = 1000

groups_Nro_Doc = [values_Nro_Doc[i:i + group_size] for i in range(0, len(values_Nro_Doc), group_size)]
 

variables_grupos = []
 
for i, groups_Nro_Doc in enumerate(groups_Nro_Doc, start=1):
    #print(f"Grupo ruc {i}: {','.join(groups_ruc)}")
    globals()[f"Grupo_Nro_Doc_{i}"]=f"{','.join(groups_Nro_Doc)}"
    x=globals()[f"Grupo_Nro_Doc_{i}"]
    Query_Escalera_OAC=leer_sql(ruta_Pagos_OAC)
    Query_Escalera_OAC=Query_Escalera_OAC.format(Nro_Doc=x)
    IngresarDatos(Conexion_OAC,Query_Escalera_OAC,Conexion_Opercom,leer_sql(ruta_Insertar_Data_Tabla))
 
 
ejecutar_consultas(Actualizar_Pagos_PII,Conexion_Opercom)
ejecutar_consultas(Creacion_Historico_y_Pagos,Conexion_Opercom)
ejecutar_consultas(Creacion_Efectividades_Corp,Conexion_Opercom)

origen_cursor.close
destino_cursor.close
close_connection_db(Conexion_OAC)
close_connection_db(Conexion_Opercom)
