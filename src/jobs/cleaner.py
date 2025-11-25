import pandas as pd
from datetime import datetime
import os

TODAY = datetime(2025, 11, 22) 

def normalize_text(df):
    """Normaliza campos de texto: minúsculas y estandarización de valores."""
    
    text_cols = ['comuna', 'religion', 'canal_compra', 'tipo_alimentacion']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    df['religion'] = df['religion'].str.replace('testigos de jehová', 'testigos_jehova', regex=False)
    df['canal_compra'] = df['canal_compra'].str.replace('ambos', 'app_y_local', regex=False)
    df['tipo_alimentacion'] = df['tipo_alimentacion'].str.replace('no aplica', 'no_aplica', regex=False)
    
    print("  -> Texto normalizado.")
    return df


def standardize_dates(df):
    """Estandariza fechas y calcula la edad."""
    
    df['fecha_ultima_compra'] = pd.to_datetime(df['fecha_ultima_compra'], errors='coerce')
    
    if 'fecha_nacimiento' in df.columns:
        df['edad'] = (TODAY - df['fecha_nacimiento']).dt.days // 365
        df.drop(columns=['fecha_nacimiento'], inplace=True)
        
    print("  -> Fechas estandarizadas y Edad calculada.")
    return df


def handle_nulls(df):
    """Maneja valores nulos según las políticas de limpieza."""
    
    df['edad'] = df['edad'].fillna(-1).astype(int)
    
    df['fecha_ultima_compra'] = df['fecha_ultima_compra'].fillna(datetime(1900, 1, 1))

    df['canal_compra'] = df['canal_compra'].replace('nulo', 'desconocido') 
    
    print("  -> Nulos manejados.")
    return df


def process_to_silver(df_bronze):
    """
    Ejecuta todo el proceso de limpieza y produce el DataFrame Silver.
    Recibe df_bronze directamente en memoria.
    """
    print("Iniciando proceso de limpieza (Capa Silver)...")
    
    if df_bronze is None:
        raise ValueError("El DataFrame de la capa Bronze no puede ser nulo.")

    df_silver = df_bronze.copy()
    
    df_silver = normalize_text(df_silver)
    df_silver = standardize_dates(df_silver)
    df_silver = handle_nulls(df_silver)
    
    df_silver['tipo_cliente'] = df_silver['tipo_cliente'].astype(int)
    df_silver['promedio_compras'] = pd.to_numeric(df_silver['promedio_compras'])

    OUTPUT_PATH = "data/silver/ventas/clientes_limpio.parquet"
    OUTPUT_DIR = os.path.dirname(OUTPUT_PATH)
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"Directorio creado: {OUTPUT_DIR}")

    df_silver.to_parquet(OUTPUT_PATH, index=False)
    print(f"✅ Proceso de Silver completado. Archivo guardado en: {OUTPUT_PATH}")
    
    return df_silver