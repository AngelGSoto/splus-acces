import pandas as pd
import splusdata
import os
import logging
import time
from getpass import getpass
from contextlib import contextmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from astropy.table import Table
from astropy.io import fits
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("splus_query.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('SPLUS_QUERY')

# Configuración de constantes
MAX_RETRIES = 10
RETRY_DELAY = 10
BATCH_SIZE = 100000

@contextmanager
def create_connection(username=None, password=None):
    """Manejo seguro de conexión para versiones antiguas de splusdata"""
    session = requests.Session()
    
    # Configurar política de reintentos
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    
    try:
        conn = splusdata.Core(
            username=username or os.getenv('SPLUS_USERNAME'),
            password=password or os.getenv('SPLUS_PASSWORD')
        )
        # Inyectar la sesión personalizada
        conn.session = session
        yield conn
    finally:
        session.close()

def build_query(field):
    """Consulta optimizada y segura"""
    return f"""
    SELECT 
        dual.Field, dual.ID, dual.RA, dual.DEC,
        dual.X, dual.Y,
        dual.PETRO_RADIUS, dual.KRON_RADIUS,
        dual.s2n_DET_PStotal, dual.s2n_r_PStotal, s2n_J0660_PStotal, dual.s2n_i_PStotal, dual.SEX_FLAGS_DET, 
        dual.SEX_FLAGS_r, dual.SEX_FLAGS_i, dual.SEX_FLAGS_J0660,
        dual.r_PStotal, dual.e_r_PStotal,
        dual.g_PStotal, dual.e_g_PStotal,
        dual.i_PStotal, dual.e_i_PStotal,
        dual.u_PStotal, dual.e_u_PStotal,
        dual.z_PStotal, dual.e_z_PStotal,
        dual.j0378_PStotal, dual.e_j0378_PStotal,
        dual.j0395_PStotal, dual.e_j0395_PStotal,
        dual.j0410_PStotal, dual.e_j0410_PStotal,
        dual.j0430_PStotal, dual.e_j0430_PStotal,
        dual.j0515_PStotal, dual.e_j0515_PStotal,
        dual.j0660_PStotal, dual.e_j0660_PStotal,
        dual.j0861_PStotal, dual.e_j0861_PStotal,
        psf.r_psf, psf.e_r_psf,
        psf.g_psf, psf.e_g_psf,
        psf.i_psf, psf.e_i_psf,
        psf.u_psf, psf.e_u_psf,
        psf.z_psf, psf.e_z_psf,
        psf.j0378_psf, psf.e_j0378_psf,
        psf.j0395_psf, psf.e_j0395_psf,
        psf.j0410_psf, psf.e_j0410_psf,
        psf.j0430_psf, psf.e_j0430_psf,
        psf.j0515_psf, psf.e_j0515_psf,
        psf.j0660_psf, psf.e_j0660_psf,
        psf.j0861_psf, psf.e_j0861_psf,
        sgq.CLASS, sgq.PROB_STAR, sgq.PROB_QSO, sgq.PROB_GAL
    FROM "idr5"."idr5_dual" AS dual
        LEFT JOIN "idr5"."idr5_psf" AS psf USING(ID)
        LEFT JOIN "idr5_vacs"."idr5_sqg" AS sgq USING(ID)
    WHERE 
        e_J0395_PStotal <= 0.3 
        AND e_J0410_PStotal <= 0.3 
        AND e_J0430_PStotal <= 0.3 
        AND e_g_PStotal <= 0.3  
        AND e_J0515_PStotal <= 0.3 
        AND e_r_PStotal <= 0.3 
        AND e_J0660_PStotal <= 0.3
        AND e_i_PStotal <= 0.3 
        AND e_J0861_PStotal <= 0.3 
        AND e_z_PStotal <= 0.3
        AND CLASS = 1 AND
        dual.Field = '{field}'
    """

def process_field(conn, field, output_dir, failed_fields):
    """Procesa un campo con manejo de errores"""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Procesando campo {field}, intento {attempt+1}")
            query = build_query(field)
            logger.debug(f"Consulta SQL para {field}: {query}")
            df = conn.query(query).to_pandas()
            
            if not df.empty:
                # Imprimir el número de objetos encontrados
                num_objects = len(df)
                logger.info(f"Campo {field} tiene {num_objects} objetos.")
                
                # Optimizar uso de memoria
                columns_to_convert = {
                    'RA': 'float32',
                    'DEC': 'float32',
                    'r_PStotal': 'float32',
                    'e_r_PStotal': 'float32'
                }
                for col, dtype in columns_to_convert.items():
                    if col in df.columns:
                        df[col] = df[col].astype(dtype)
                
                # Guardar en formato eficiente
                output_file = os.path.join(output_dir, f"{field}.parquet")
                df.to_parquet(output_file)
                return output_file
            else:
                logger.warning(f"El resultado de la consulta para el campo {field} está vacío.")
                
        except Exception as e:
            logger.error(f"Intento {attempt+1} fallido para {field} con error: {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    logger.warning(f"Campo {field} no procesado después de {MAX_RETRIES} intentos")
    failed_fields.append(field)
    return None

def main(test_mode=False):
    """Función principal modificada para guardar en FITS"""
    start_time = time.time()
    output_dir = "splus_results"
    os.makedirs(output_dir, exist_ok=True)
    
    failed_fields = []
    
    # Cargar campos
    fields = pd.read_csv("iDR5_pointings.csv")
    if test_mode:
        fields = fields.sample(20)
        logger.info("Modo prueba activado - 2 campos aleatorios")
    
    with create_connection() as conn:
        processed_data = []
        
        with ThreadPoolExecutor(max_workers=12) as executor:
            future_to_field = {executor.submit(process_field, conn, field, output_dir, failed_fields): field for field in fields["iDR5_Field_Name"]}
            for future in as_completed(future_to_field):
                field = future_to_field[future]
                try:
                    output_file = future.result()
                    if output_file:
                        logger.info(f"Archivo guardado para el campo {field}: {output_file}")
                        df = pd.read_parquet(output_file)
                        processed_data.append(df)
                except Exception as e:
                    logger.error(f"Error procesando {field}: {str(e)}")
    
    logger.info("Finalizando procesamiento de campos.")
    
    # Combinar todos los datos
    if processed_data:
        logger.info("Combinando DataFrames procesados.")
        final_df = pd.concat(processed_data, ignore_index=True)
        
        # Verificar que el DataFrame no esté vacío
        if final_df.empty:
            logger.error("El DataFrame combinado está vacío. No se generará el archivo FITS.")
        else:
            logger.info("DataFrame combinado no está vacío, procediendo a generar el archivo FITS.")
            
            # Convertir a tabla Astropy
            astropy_table = Table.from_pandas(final_df)
            
            # Verificar la tabla Astropy
            if len(astropy_table) == 0:
                logger.error("La tabla Astropy está vacía. No se generará el archivo FITS.")
            else:
                logger.info(f"La tabla Astropy tiene {len(astropy_table)} filas. Procediendo a escribir el archivo FITS.")
                
                # Eliminar metadatos problemáticos
                astropy_table.meta.clear()
                
                # Crear archivo FITS
                output_file = f"splus_data_iDR5_stars_{time.strftime('%Y%m%d%H%M')}.fits"
                astropy_table.write(output_file, format='fits', overwrite=True)
                
                logger.info(f"Archivo FITS creado: {output_file}")
                logger.info(f"Tamaño del archivo: {os.path.getsize(output_file)/1024**2:.2f} MB")
    
    # Guardar campos fallidos en un archivo CSV
    if failed_fields:
        failed_df = pd.DataFrame(failed_fields, columns=["Field"])
        failed_df.to_csv("failed_fields.csv", index=False)
        logger.info(f"Campos fallidos guardados en failed_fields.csv")
    
    logger.info(f"Tiempo total: {(time.time()-start_time)/60:.1f} minutos")

if __name__ == "__main__":
    main(test_mode=False)
