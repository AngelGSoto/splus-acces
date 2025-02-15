import pandas as pd
import splusdata
import re
from getpass import getpass
from concurrent.futures import ThreadPoolExecutor, as_completed
from astropy.table import Table
import logging

# Configuración
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SPLUS_COUNTER')
MAX_WORKERS = 8
OUTPUT_FITS = "field_counts.fits"

def process_field(args):
    """Procesamiento robusto con validación de nombres"""
    conn, field = args
    original_field = field.strip()
    
    # Intentar 3 formatos de nombres diferentes
    variants = [
        original_field,
        original_field.replace("_", "-"),
        original_field.replace("-", "_")
    ]
    
    for variant in variants:
        try:
            clean_field = re.sub(r"[^\w-]", "", variant)
            query = f'SELECT COUNT(*) AS total FROM "idr5"."idr5_dual" WHERE "Field" = \'{clean_field}\''
            result = conn.query(query)
            count = result['total'][0]
            if count > 0:
                return (original_field, count)
        except Exception as e:
            continue
            
    return (original_field, 0)

def main():
    # Conexión
    user = input("Usuario SPLUS: ")
    pwd = getpass("Contraseña SPLUS: ")
    conn = splusdata.Core(user, pwd)
    
    # Leer campos
    fields = pd.read_csv("iDR5_pointings.csv")["iDR5_Field_Name"].tolist()
    
    # Procesamiento paralelo
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_field, (conn, f)) for f in fields]
        
        for future in as_completed(futures):
            field, count = future.result()
            print(f"Campo {field}: {count} objetos")
            results.append({"Field": field, "Count": count})
    
    # Guardar resultados
    Table(rows=results).write(OUTPUT_FITS, overwrite=True)
    total = sum(item["Count"] for item in results)
    print(f"\nTOTAL GLOBAL: {total} objetos")

if __name__ == "__main__":
    main()
