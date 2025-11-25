import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest.data_loader import ingest_to_bronze
from jobs.cleaner import process_to_silver

def run_data_pipeline():
    """
    Ejecuta el pipeline completo de Ingesta (Bronze) y Limpieza (Silver).
    """
    print("=" * 40)
    print("Pipeline de Datos: Equipo A (Bronze & Silver)")
    print("=" * 40)
    
    try:
        print("\n[FASE BRONZE] Ingesta y validación de 500 registros.")
        df_bronze = ingest_to_bronze()
        
        print("\n[FASE SILVER] Aplicando limpieza (Spark/Pandas).")
        df_silver = process_to_silver(df_bronze)
        
        print("\n--- Vista de datos finales (Silver) ---")
        print(df_silver[['nombre', 'apellido', 'edad', 'comuna', 'tipo_alimentacion', 'promedio_compras']].head(5))

    except Exception as e:
        print(f"\n❌ Error Crítico en el Pipeline: {e}")

if __name__ == "__main__":
    run_data_pipeline()