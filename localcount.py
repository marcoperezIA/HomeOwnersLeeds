import os
import pandas as pd

# Ruta donde están los CSV
OUTPUT_DIR = r"C:\Users\IA\Desktop\HomeOwnersLeeds\outputs_chunks"

total_registros = 0

for file in os.listdir(OUTPUT_DIR):
    if file.endswith(".csv"):
        path = os.path.join(OUTPUT_DIR, file)
        try:
            df = pd.read_csv(path)
            count = len(df)
            total_registros += count
            print(f"{file}: {count} registros")
        except Exception as e:
            print(f"Error leyendo {file}: {e}")

print("\n==========================")
print(f"📊 Total registros en todos los CSV: {total_registros}")
