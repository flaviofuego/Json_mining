import pandas as pd
import json
import os
from pandas_gbq import to_gbq

# Ruta de la carpeta con los archivos JSON
json_folder = 'json'

# Listas para almacenar los datos
activity_ids = []
json_contents = []

# Leer todos los archivos JSON de la carpeta
for filename in os.listdir(json_folder):
    if filename.endswith('.json'):
        # Obtener el nombre sin extensión
        activity_id = os.path.splitext(filename)[0]
        
        # Leer el contenido del archivo JSON
        file_path = os.path.join(json_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            json_content = json.load(f)
            # Convertir a string
            json_string = json.dumps(json_content, ensure_ascii=False)
        
        activity_ids.append(activity_id)
        json_contents.append(json_string)

# Crear el DataFrame
df = pd.DataFrame({
    'activityid': activity_ids,
    'json_content': json_contents
})


print(f"Total de archivos procesados: {len(df)}")
print(df.head())

# Cargar a BigQuery
to_gbq(df, 'json_dbt.tabla_base', project_id='gen-lang-client-0721975631', if_exists='replace')
print("Datos cargados exitosamente a BigQuery") 