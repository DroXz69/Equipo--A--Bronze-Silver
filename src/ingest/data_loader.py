import pandas as pd
import re
from io import StringIO
from datetime import datetime
import os
import glob

RAW_DIR = "data/raw"
OUTPUT_PATH = "data/bronze/ventas/clientes_ingesta.parquet"


def get_file_content(file_name):
    """Función auxiliar para leer el contenido de un archivo."""
    file_path = os.path.join(RAW_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"El archivo {file_name} no se encontró en la ruta: {RAW_DIR}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()


def load_sql_data():
    """Carga y parsea el contenido del archivo SQL leyendo del disco."""
    sql_content = get_file_content("clientes.sql")
    
    pattern = re.compile(r"INSERT INTO clientes VALUES \((.*?)\);")
    matches = pattern.findall(sql_content)
    
    data_sql = []
    for match in matches:
        values = [v.strip().replace("'", "") for v in match.split(',')]
        data_sql.append(values)

    cols_clientes = ['codigo', 'nombre', 'apellido', 'comuna', 'rut', 'fecha_nacimiento', 'religion']
    df_sql = pd.DataFrame(data_sql, columns=cols_clientes)
    df_sql['codigo'] = pd.to_numeric(df_sql['codigo'])
    df_sql['fecha_nacimiento'] = pd.to_datetime(df_sql['fecha_nacimiento'], errors='coerce')
    return df_sql


def load_txt_data():
    """Carga y parsea el contenido del archivo TXT leyendo del disco."""
    txt_content = get_file_content("clientes_extra.txt")
    
    lines = [line.strip() for line in txt_content.split('\n') if line.strip()]
    data_txt = []
    
    for line in lines:
        line_clean = re.sub(r'\s*', '', line).strip()
        parts = [part.strip() for part in line_clean.split(',')]
        
        if len(parts) == 4:
            data_txt.append(parts)
        elif len(parts) == 3 and parts[0] == '34':
            data_txt.append([parts[0], '', parts[1], parts[2]])
        elif len(parts) == 4 and parts[1] == ' ':
            data_txt.append([parts[0], '', parts[2], parts[3]]) 
        else:
            data_txt.append(parts)

    cols_extra = ['codigo', 'canal_compra', 'codigo_venta', 'fecha_ultima_compra']
    df_extra = pd.DataFrame(data_txt, columns=cols_extra)
    df_extra['codigo'] = pd.to_numeric(df_extra['codigo'])
    df_extra['fecha_ultima_compra'] = pd.to_datetime(df_extra['fecha_ultima_compra'], errors='coerce')
    df_extra['canal_compra'] = df_extra['canal_compra'].replace('', 'Nulo').str.strip()
    return df_extra


def load_csv_data():
    """Carga el contenido del archivo CSV leyendo del disco."""
    csv_path = os.path.join(RAW_DIR, "clientes_info.csv")
    df_info = pd.read_csv(csv_path)
    return df_info


def ingest_to_bronze():
    """
    Ejecuta la ingesta de los 3 archivos leyendo de data/raw y 
    lo guarda en data/bronze/ventas/.
    """
    
    if not os.path.exists(RAW_DIR):
        raise FileNotFoundError(f"ERROR: La carpeta de datos crudos ('{RAW_DIR}') no existe. Por favor, créala y coloca los archivos de datos.")

    df_sql = load_sql_data()
    df_txt = load_txt_data()
    df_csv = load_csv_data()
    
    df_bronze = df_sql.merge(df_csv, left_on='codigo', right_on='codigo_cliente', how='inner').drop(columns=['codigo_cliente'])
    df_bronze = df_bronze.merge(df_txt, on='codigo', how='inner')
    
    if len(df_bronze) != 500:
        print(f"⚠️ Alerta: Se esperaban 500 registros, se encontraron {len(df_bronze)}.")

    OUTPUT_DIR = os.path.dirname(OUTPUT_PATH)

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"Directorio creado: {OUTPUT_DIR}")

    df_bronze.to_parquet(OUTPUT_PATH, index=False)
    
    print(f"✅ Ingesta a Capa Bronze completada con {len(df_bronze)} registros.")
    print(f"Archivo guardado exitosamente en: {OUTPUT_PATH}")
    
    return df_bronze