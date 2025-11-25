from typing import Optional, Tuple, Any, Mapping
import os
import yaml
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
import time
from datetime import datetime, date, timedelta
import sys
import openpyxl
import shutil
import gspread
from google.oauth2.service_account import Credentials
from typing import Union
#from mstrio.connection import Connection
#from mstrio.project_objects import Report

########################################################################################
## CARGA PERFIL DE EJECUCION                                                        ####
########################################################################################

def load_config(profile: str = None, config_path: str = "config.yaml") -> dict:
    """
    Carga la configuración desde config.yaml.
    - Mergea el bloque 'shared' con el perfil seleccionado.
    - Permite definir el perfil también por la variable de entorno APP_PROFILE.
    - Imprime el perfil que quedó activo.
    """
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    # Si no se pasa explícito, se toma de variable de entorno o del default
    profile = profile or os.getenv("APP_PROFILE") or raw.get("default_profile", "test")
    if "profiles" not in raw or profile not in raw["profiles"]:
        raise ValueError(f"Perfil '{profile}' no encontrado en {config_path}")

    shared = raw.get("shared", {}) or {}
    prof = raw["profiles"][profile] or {}

    def deep_merge(a: dict, b: dict) -> dict:
        out = dict(a)
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = deep_merge(out[k], v)
            else:
                out[k] = v
        return out

    merged = deep_merge(shared, prof)

    # 🔹 Nuevo: imprimir perfil activo
    print(f"Perfil en ejecución: {profile}")

    return merged

##########################################################################################
## COPIAS RAW DE ESTADOS INTERMEDIOS                                                  ####
##########################################################################################

##########################################################################################
## CONEXION GOOGLE SHEET Y GENERACION DF                                              ####
##########################################################################################

# Scopes de solo lectura
#"https://www.googleapis.com/auth/spreadsheets.readonly",
#"https://www.googleapis.com/auth/drive.readonly",

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def make_client(service_account_json: str):
    creds = Credentials.from_service_account_file(service_account_json, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc

def read_sheet_to_df(
    gc,
    sheet_url: str,
    worksheet: str,
    range_a1: Optional[str] = None,
    assume_header: bool = True,
) -> pd.DataFrame:
    """
    ----------
    gc : gspread.Client
        Cliente autenticado de gspread (creado con `make_client`).
    sheet_url : str
        URL completa del Google Sheet (ejemplo: 
        "https://docs.google.com/spreadsheets/d/.../edit#gid=0").
    worksheet : str
        Nombre de la hoja/tab dentro del Google Sheet (ejemplo: "Mercadologico Category").
    range_a1 : str, opcional
        Rango en notación A1 a leer (ejemplo: "A:I" o "A1:C100").  
        Si se deja en `None`, se traerá **toda la hoja** (`get_all_values()`).
    assume_header : bool, default=True
        - `True`: La primera fila se interpreta como **encabezado** y se usa como nombres de columnas.  
        - `False`: No se asume encabezado; todas las filas se devuelven como datos y las columnas quedan numeradas automáticamente.
    -------
    """
    sh = gc.open_by_url(sheet_url)
    ws = sh.worksheet(worksheet)
    values = ws.get(range_a1) if range_a1 else ws.get_all_values()

    if not values:
        return pd.DataFrame()

    if assume_header:
        header, data = values[0], values[1:]
        df = pd.DataFrame(data, columns=header)
    else:
        df = pd.DataFrame(values)

    return df

def write_gsheet(df, spreadsheet_url, worksheet_name, gc, cell_range=None, clean:bool=False,webhook=object):
    """
    Escribe un DataFrame en una hoja de Google Sheets.

    Parámetros:
        df (pd.DataFrame): DataFrame a escribir.
        spreadsheet_url (str): URL completa del Google Spreadsheet.
        worksheet_name (str): Nombre de la hoja dentro del Spreadsheet.
        gc (gspread.Client): Cliente autenticado de gspread.
        cell_range (str, opcional): Rango de celdas a actualizar (ej: "A1:C10").
                                    Si no se especifica, se sobrescribe toda la hoja.
        clean (bool): Si True, limpia el rango antes de inyectar.
        webhook (obj): Objeto con método `.send(msg)` para enviar alertas en caso de error por hangouts.

    """
    try:
        spreadsheet = gc.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Determinar rango a limpiar
        if clean:
            if cell_range:
                worksheet.batch_clear([cell_range])
            else:
                worksheet.clear()

        # Limpiar valores no válidos
        df_clean = df.fillna('')
        #.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], '')
        #df_clean = df_clean.where(df_clean.notnull(), None)
        #data = [df.columns.values.tolist()] + df.values.tolist()
        # Solo los datos
        data = df_clean.values.tolist()

        if cell_range:
            worksheet.update(cell_range, data)
        else:
            worksheet.update(data)

        print(f"La hoja {worksheet_name} en el rango {cell_range} fue actualizada.")

    except Exception as e:
        #msg = f"FLUJO CARGA_PROYECCION | Error al escribir en Google Sheets: {str(e)}"
        #webhook.send(msg)
        print(f"FLUJO CARGA_ODOP | Error al escribir en Google Sheets: {str(e)}")

########################################################################################
##  TRANSFORMACION                                                                  ####
########################################################################################

########################################################################################
##  CONTROL Y ALERTA                                                                 ###
########################################################################################

########################################################################################
## CONEXION DE MICROSTRATEGY                                                        ####
########################################################################################

########################################################################################
## AUXILIARES ESPECIFICAS                                                           ####
########################################################################################

########################################################################################
## SNOWFLAKE, CONSULTAS, INYECCION                                                  ####
########################################################################################

def snowflake_login(user: str, password: str, account: str, database: str, schema: str, require_passcode: bool = False):
    print('')
    print('Conexion Snowflake')

    counter = 0
    cursor = None
    snowflake_connection = None

    while True:
        if counter + 1 < 4:
            print(f"Intento {counter + 1}")

            try:
                pass_args = {}
                if require_passcode:
                    pass_ = input("INGRESAR PASSCODE: ")
                    pass_args["passcode"] = pass_

                # Establish Snowflake connection
                snowflake_connection = snowflake.connector.connect(
                    user=user,
                    password=password,
                    account=account,
                    database=database,
                    schema=schema,
                    **pass_args
                )

                cursor = snowflake_connection.cursor()
                print('Conectado a SNOWFLAKE')
                break

            except Exception as e:
                counter += 1
                print(f'Error: {e}')
                if require_passcode:
                    print('Incorrect Password - provide again')
                else:
                    break  # si no hay MFA, no tiene sentido reintentar

        else:
            print('3 Intentos fallidos')
            break

    print('')
    return user, cursor, snowflake_connection

# def descargar_query_cond(cursor: snowflake.connector.cursor.SnowflakeCursor,
#                        query: str, cond = None) -> pd.DataFrame:
#     """
#     cursor : snowflake.connector.cursor.SnowflakeCursor
#             Cursor activo de Snowflake para ejecutar la consulta.
#         query : str
#             Nombre del archivo SQL. Se buscará como '<query>.sql'.
#         cond : str, opcional
#             Condición adicional para reemplazar el ';' en la query (por ejemplo cláusulas WHERE).
    
#     Retorno
#     -------
#     pd.DataFrame
#         DataFrame con el resultado de la consulta.

#     Notas
#     -----
#     - El archivo SQL debe estar en el mismo directorio donde se ejecute la función,
#       o se debe pasar `query` con la ruta relativa/absoluta.
#     - Si `cond` no se especifica, se ejecuta la query tal cual está en el archivo.
#     - Si `cond` se pasa, reemplaza el `;` final de la query por el texto de la condición.
#     """
#     query_path = query #+ '.sql'

    
#     with open(query_path, 'r', encoding="utf8") as file: command = file.read()

#     if not(cond):
#         cursor.execute(command)
#     else:
#         cursor.execute(command.replace(';', cond))    
    
#     df = cursor.fetch_pandas_all()

#     return df

def descargar_query_cond(cursor: snowflake.connector.cursor.SnowflakeCursor, 
                         query: str, 
                         cond: Union[str, None] = None,
                         **params) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL con parámetros dinámicos.

    Parámetros:
    -----------
    cursor : SnowflakeCursor
        Cursor activo de Snowflake.
    query : str
        Ruta al archivo SQL.
    cond : str, opcional
        Condición adicional para reemplazar el ';' final en el SQL.
    **params : dict
        Parámetros opcionales que reemplazarán los placeholders {var} en el SQL.

    Retorna:
    --------
    pd.DataFrame
    """
    with open(query, 'r', encoding='utf8') as file:
        command = file.read()

    # Reemplazar placeholders {var}
    if params:
        command = command.format(**params)

    # Reemplazar ';' si se pasa condicional
    if cond:
        command = command.replace(';', cond)

    cursor.execute(command)
    return cursor.fetch_pandas_all()
