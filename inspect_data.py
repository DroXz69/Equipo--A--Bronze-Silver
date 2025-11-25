import pandas as pd
import os

BRONZE_PATH = "data/bronze/ventas/clientes_ingesta.parquet"
SILVER_PATH = "data/silver/ventas/clientes_limpio.parquet"

def inspect_pipeline_results():
    """Inspecciona y muestra el contenido de las capas Bronze y Silver."""
    
    print("=" * 50)
    print("VERIFICACIÓN DE ARCHIVOS DEL PIPELINE")
    print("=" * 50)

    # --- A. INSPECCIÓN DE LA CAPA BRONZE ---
    if os.path.exists(BRONZE_PATH):
        try:
            df_bronze = pd.read_parquet(BRONZE_PATH)
            print("\n✅ DATOS DE LA CAPA BRONZE (INGESTA CRUDA)")
            print(f"Número de Registros: {len(df_bronze)}")
            print("Muestra (Incluye fecha y texto sin normalizar):")
            print(df_bronze[['nombre', 'religion', 'canal_compra', 'fecha_nacimiento']].head())
            print("\n" + "-"*15)
        except Exception as e:
            print(f"❌ Error al leer Bronze: {e}")
    else:
        print(f"❌ Error: Archivo Bronze no encontrado en {BRONZE_PATH}")


    # --- B. INSPECCIÓN DE LA CAPA SILVER ---
    if os.path.exists(SILVER_PATH):
        try:
            df_silver = pd.read_parquet(SILVER_PATH)
            print("\n✅ DATOS DE LA CAPA SILVER (LIMPIEZA FINAL)")
            print(f"Número de Registros: {len(df_silver)}")
            print("Muestra (Verifica Edad y Texto Normalizado):")
            print(df_silver[['nombre', 'religion', 'canal_compra', 'edad', 'promedio_compras']].head())
            print("\n" + "-"*15)

            num_lower = df_silver[df_silver['religion'].str.islower()].shape[0]
            print(f"Validación de Normalización de Texto (Minúsculas en religión): {num_lower} / {len(df_silver)} registros.")
            
        except Exception as e:
            print(f"❌ Error al leer Silver: {e}")
    else:
        print(f"❌ Error: Archivo Silver no encontrado en {SILVER_PATH}")

if __name__ == "__main__":
    inspect_pipeline_results()