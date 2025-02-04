import pandas as pd
import os
import logging
import time
from astropy.table import Table, vstack
from astropy.io import fits
import pyarrow.parquet as pq

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fits_creation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FITS_CREATOR')

def create_final_fits(output_dir, final_filename, chunk_size=100000):
    """Crea un único FITS con una sola tabla combinada"""
    start_time = time.time()
    
    # Obtener lista de archivos Parquet
    parquet_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.parquet')]
    
    if not parquet_files:
        logger.error("No se encontraron archivos Parquet")
        return

    parquet_files.sort()
    master_table = None  # Tabla maestra acumulativa
    total_rows = 0

    try:
        for i, file_path in enumerate(parquet_files):
            logger.info(f"Procesando {i+1}/{len(parquet_files)}: {os.path.basename(file_path)}")
            
            parquet_file = pq.ParquetFile(file_path)
            
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                df = batch.to_pandas()
                
                # Convertir batch a tabla Astropy
                current_table = Table.from_pandas(df)
                
                # Combinar tablas
                if master_table is None:
                    master_table = current_table
                else:
                    master_table = vstack([master_table, current_table])
                
                total_rows += len(current_table)
                logger.debug(f"Filas acumuladas: {total_rows}")
                
                # Liberar memoria intermedia
                del current_table, df

        # Escribir archivo FITS final
        if master_table is not None:
            logger.info("Escribiendo archivo FITS...")
            master_table.write(final_filename, format='fits', overwrite=True)
            logger.info(f"FITS creado: {final_filename}")
            logger.info(f"Filas totales: {total_rows}")
            logger.info(f"Tamaño: {os.path.getsize(final_filename)/1024**3:.2f} GB")
        else:
            logger.warning("No hay datos para escribir")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        if os.path.exists(final_filename):
            os.remove(final_filename)
            logger.warning("Archivo FITS corrupto eliminado")
        raise

    finally:
        # Liberar memoria
        if master_table is not None:
            del master_table

    logger.info(f"Tiempo total: {(time.time()-start_time)/60:.1f} minutos")

if __name__ == "__main__":
    output_dir = "splus_results_psf"
    final_filename = f"splus_data_iDR5_PSF_Disk_MC_{time.strftime('%Y%m%d%H%M')}.fits"
    create_final_fits(output_dir, final_filename)
