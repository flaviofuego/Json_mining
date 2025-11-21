# Importar librerías necesarias
import pandas as pd
import json
import os
from pandas_gbq import to_gbq
from dotenv import load_dotenv
import glob

# Cargar variables de entorno
load_dotenv()

# Configurar proyecto de BigQuery
project_id = "gen-lang-client-0721975631"
dataset_id = "Ordenes"  # Cambia esto al dataset que quieras usar
table_name = "publicaciones_raw"  # Nombre de la tabla destino

# Función para procesar un archivo JSON
def process_json_file(file_path):
    # Extraer el activity_id del nombre del archivo (ej: 13199.json -> 13199)
    activity_id = os.path.splitext(os.path.basename(file_path))[0]
    
    # Leer el contenido del JSON como string
    with open(file_path, 'r', encoding='utf-8') as f:
        json_raw = f.read()
    
    return {
        'activity_id': activity_id,
        'json_raw': json_raw
    }

# Procesar todos los archivos JSON en la carpeta json/
json_folder = 'json'
json_files = glob.glob(os.path.join(json_folder, '*.json'))

print(f"Se encontraron {len(json_files)} archivos JSON")

# Lista para almacenar todos los registros
all_records = []

for json_file in json_files:
    print(f"Procesando: {json_file}")
    try:
        record = process_json_file(json_file)
        all_records.append(record)
    except Exception as e:
        print(f"Error procesando {json_file}: {str(e)}")

# Crear DataFrame
df = pd.DataFrame(all_records)

print(f"\nDataFrame creado con {len(df)} registros")
print("\nPrimeras filas:")
print(df.head())

# Subir a BigQuery
print(f"\nSubiendo datos a BigQuery: {dataset_id}.{table_name}")
try:
    to_gbq(
        df, 
        f'{dataset_id}.{table_name}', 
        project_id=project_id, 
        if_exists='replace',  # Cambia a 'append' si quieres agregar datos
        location='US'
    )
    print(f"✓ Datos cargados exitosamente a {dataset_id}.{table_name}")
except Exception as e:
    print(f"Error al cargar a BigQuery: {str(e)}")

# Verificar la carga
from pandas_gbq import read_gbq
query = f"""
SELECT COUNT(*) as total_registros
FROM `{project_id}.{dataset_id}.{table_name}`
"""
df_count = read_gbq(query, project_id=project_id, dialect="standard", location="US")
print(f"\nTotal de registros en BigQuery: {df_count['total_registros'].iloc[0]}")