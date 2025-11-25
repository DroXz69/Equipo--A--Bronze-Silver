# 🚀 Proyecto de Ingesta y Limpieza de Datos (Equipo A: Bronze & Silver)

Este proyecto implementa un pipeline de datos siguiendo una arquitectura de **Lakehouse** con dos etapas: Ingesta (Capa Bronze) y Limpieza (Capa Silver). El objetivo es procesar información de clientes desde múltiples fuentes (SQL, CSV, TXT) para crear un conjunto de datos limpio y estandarizado listo para el análisis.

---

## 🏗️ Arquitectura del Flujo de Datos

El pipeline se ejecuta secuencialmente, moviendo los datos entre capas:

1.  **Capa Bronze (Ingesta):**
    * **Función:** Ingesta 500 registros combinados leyendo los archivos de la carpeta **`data/raw/`**.
    * **Proceso:** Carga, parsea y une los datos (SQL, CSV, TXT), realizando validaciones básicas.
    * **Destino:** Los datos crudos se guardan en el *Data Lake* en el directorio `/data/bronze/ventas/` como el archivo `clientes_ingesta.parquet`.

2.  **Capa Silver (Limpieza):**
    * **Función:** Aplica transformaciones de calidad de datos y estandarización.
    * **Proceso:** Simulación de un proceso Spark (implementado con Pandas en el código) para:
        * **Normalizar texto** (e.g., minúsculas, estandarización de religiones y canales de compra).
        * **Estandarizar fechas** y calcular la nueva columna **`edad`**.
        * **Manejar nulos** (e.g., fechas y edad).
    * **Destino:** Los datos limpios se guardan en `/data/silver/ventas/` como el archivo `clientes_limpio.parquet`.

---

## 👤👤 Equipo de Integrantes. 👤👤

`Equipo A Bronze/Silver 5 Diciembre.`

* **Daniel Garrido**
* **Víctor Faúndez**
* **Camilo Jeldres**
* **Bruno Polo**

`Sección: TI2081/D-IEI-N8-P1-C2/D Renca IEI`

`Big Data`

---

## 🛠️ Requisitos e Instalación

Para ejecutar el pipeline, necesitarás **Python 3.8+** y las dependencias listadas en `requirements.txt`.

### 🚨 1. Preparación del Entorno (Una única vez)

1.  **Crear el entorno virtual:**
    ```bash
    python3 -m venv venv
    ```

2. **Activación del Entorno:**
    ```powershell
    .\venv\Scripts\Activate.ps1
    ```

3.  **Instalar dependencias:** (Asegúrate de que la instalación incluya `pyarrow` para el formato Parquet)
    ```bash
    pip install -r requirements.txt
    ```

## 💥 Para ejecutar el codigo utilizar este comando

Para realizar la ejecución del programa sigue estos pasos:

1. **Comando para ejecución Principal**
    ```bash
    python src/orchestrator/pipeline.py
    ```

2. **Comando para visualizar 5 datos finales de los archivos creados .parquet**
    ```bash
    python inspect_data.py
    ```

---

# ❗ SI ES QUE HAY UN ENTORNO VIRTUAL CREADO SIGUE ESTOS PASOS.

1. **Desactivar el Entorno Virtual:**
    ```bash
    deactivate
    ```

2. **Eliminar la Carpeta del Entorno Antiguo:**
    ```bash
    Remove-Item -Recurse -Force venv
    ```

3. **Crear un Nuevo Entorno Virtual:**
    ```bash
    python -m venv venv
    ```

4. **Activar el Nuevo Entorno:**
    ```bash
    .\venv\Scripts\Activate.ps1
    ```

5. **Instalar las Dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

Luego de realizar estos pasos podras ejecutar el programa.

1. **Comando para ejecución Principal**
    ```bash
    python src/orchestrator/pipeline.py
    ```

2. **Comando para visualizar 5 datos finales de los archivos creados .parquet**
    ```bash
    python inspect_data.py
    ```