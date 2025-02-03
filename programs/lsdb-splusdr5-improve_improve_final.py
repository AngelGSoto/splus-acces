import requests
import splusdata
import lsdb
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, TimeoutError
from getpass import getpass
import os
import time
from astropy.table import Table
from astropy.io import fits
import numpy as np
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.DEBUG,  # Aumentar el nivel de logging a DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("splus_query.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('SPLUS_QUERY')

# Configuración de columnas (versión optimizada)
DUAL_COLUMNS = [
    "Field", "ID", "RA", "DEC", "X", "Y", "A", "B", "SEX_FLAGS_r",
    "CLASS_STAR", "r_PStotal", "e_r_PStotal", "g_PStotal",
    "e_g_PStotal", "i_PStotal", "e_i_PStotal", "u_PStotal",
    "e_u_PStotal", "z_PStotal", "e_z_PStotal",
    "J0378_PStotal", "e_J0378_PStotal", "J0395_PStotal",
    "e_J0395_PStotal", "J0410_PStotal", "e_J0410_PStotal",
    "J0430_PStotal", "e_J0430_PStotal", "J0515_PStotal",
    "e_J0515_PStotal", "J0660_PStotal", "e_J0660_PStotal",
    "J0861_PStotal", "e_J0861_PStotal"
]

def download_file(url, local_path):
    """Descargar un archivo desde una URL a un ruta local"""
    logger.info(f"Descargando {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Verificar si la solicitud tuvo éxito

    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Archivo descargado: {local_path}")

def load_catalog(conn, catalog_path, columns, filters=None):
    """Carga optimizada de catálogos"""
    logger.info(f"Cargando {catalog_path}...")
    
    catalog_links = splusdata.get_hipscats(catalog_path, headers=conn.headers)
    logger.debug(f"Obtenidos los enlaces del catálogo: {catalog_links}")
    
    # Definir rutas locales para los archivos descargados
    local_margin_path = f"{catalog_path}_margin.parquet"
    local_catalog_path = f"{catalog_path}.parquet"
    
    # Descargar los archivos
    download_file(catalog_links[0][1], local_margin_path)
    download_file(catalog_links[0][0], local_catalog_path)
    
    margin = dd.read_parquet(local_margin_path)
    catalog = dd.read_parquet(local_catalog_path)
    
    # Aplicar filtros y seleccionar columnas
    if filters:
        catalog = catalog.query(" and ".join([f"{col} {op} {val}" for col, op, val in filters]))
    catalog = catalog[columns]
    
    # Verificar el número de filas cargadas en el catálogo
    num_rows = catalog.shape[0].compute()
    logger.info(f"{catalog_path} cargado con {num_rows} filas")
    
    return catalog

def main():
    try:
        # Autenticación
        username = input("Usuario de splus.cloud: ")
        password = getpass("Contraseña de splus.cloud: ")
        
        # Crear conexión
        conn = splusdata.Core(username=username, password=password)
        logger.info("Autenticación exitosa")

        # Cargar SQG primero
        logger.info("Cargando SQG...")
        sqg = load_catalog(
            conn,
            "idr5/sqg",
            columns=["RA", "DEC", "CLASS", "PROB_STAR"],
            filters=[("CLASS", "==", 1)]
        )

        # Procesar rangos de magnitud
        mag_ranges = [(16, 19), (19, 22), (13, 16), (10, 13)]
        
        for mag_min, mag_max in mag_ranges:
            logger.info(f"\n{'#'*40}\nProcesando rango {mag_min}-{mag_max}\n{'#'*40}")
            
            try:
                # Cargar dual con filtros
                dual = load_catalog(
                    conn,
                    "idr5/dual",
                    columns=DUAL_COLUMNS[:10],
                    filters=[
                        ("r_PStotal", ">=", mag_min),
                        ("r_PStotal", "<", mag_max),
                        *[("e_" + band + "_PStotal", "<=", 0.3) 
                          for band in ['r', 'g', 'i', 'z', 'J0515', 'J0660', 'J0395', 'J0410', 'J0430', 'J0861']]
                    ]
                )
                
                # Crossmatch optimizado
                crossmatch = dual.crossmatch(sqg, radius_arcsec=1)
                df = crossmatch.compute(timeout=1800)  # 30 minutos máximo
                
                # Guardar resultados
                output_file = f"dual_sqg_{mag_min}_{mag_max}.fits"
                Table.from_pandas(df).write(output_file, overwrite=True)
                logger.info(f"Archivo guardado: {output_file}")
                
            except Exception as e:
                logger.error(f"Error en rango {mag_min}-{mag_max}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error general: {str(e)}")
    finally:
        logger.info("Proceso completado")

if __name__ == "__main__":
    # Configuración Dask para grandes volúmenes de datos
    client = Client(
        n_workers=8,
        threads_per_worker=2,
        memory_limit="4GB",
        dashboard_address=":8787"
    )
    
    try:
        main()
    finally:
        client.close()
