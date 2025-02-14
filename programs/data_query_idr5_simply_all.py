import pandas as pd
import splusdata
import os
import logging
import time
import re
from getpass import getpass
from contextlib import contextmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from astropy.table import Table
from astropy.io import fits
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
MAX_RETRIES = 5
RETRY_DELAY = 15
MAX_WORKERS = 8
PARQUET_COMPRESSION = 'snappy'

class ThreadSafeList:
    """Lista thread-safe con lock"""
    def __init__(self):
        self._list = []
        self._lock = threading.Lock()

    def append(self, value):
        with self._lock:
            self._list.append(value)

    def __iter__(self):
        with self._lock:
            return iter(self._list.copy())

@contextmanager
def create_connection(username=None, password=None):
    """Manejo seguro de conexión con política de reintentos mejorada"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=['HEAD', 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'TRACE']
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    try:
        conn = splusdata.Core(
            username=username or os.getenv('SPLUS_USERNAME'),
            password=password or os.getenv('SPLUS_PASSWORD')
        )
        conn.session = session
        yield conn
    finally:
        session.close()

def sanitize_field(field):
    """Validación mejorada que permite guiones y guiones bajos"""
    if not isinstance(field, str) or not re.match(r'^[a-zA-Z0-9_-]+$', field):
        raise ValueError(f"Nombre de campo inválido: {field} (solo se permiten: letras, números, guiones (-) y guiones bajos (_))")
    return field

def build_query(field):
    """Consulta parametrizada con campo sanitizado"""
    safe_field = sanitize_field(field)
    return f"""
    SELECT 
        psf.ID, psf.RA, psf.DEC,
        psf.g_psf, psf.e_g_psf
    FROM "idr5"."idr5_psf" AS psf
    WHERE psf.Field = '{safe_field}'
    """

def process_field(conn, field, output_dir, failed_fields):
    """Procesamiento optimizado con manejo de memoria"""
    logger.info(f"Iniciando procesamiento de campo: {field}")
    
    for attempt in range(MAX_RETRIES):
        try:
            query = build_query(field)
            df = conn.query(query).to_pandas()
            
            if df.empty:
                logger.warning(f"Campo {field} vacío - Intento {attempt+1}")
                continue
                
            # Optimización de tipos de datos
            type_map = {
                'RA': 'float32',
                'DEC': 'float32',
                'g_psf': 'float32',
                'e_g_psf': 'float32'
            }
            df = df.astype({k: v for k, v in type_map.items() if k in df})
            
            # Guardar en Parquet con compresión
            output_file = os.path.join(output_dir, f"{field}.parquet")
            df.to_parquet(output_file, compression=PARQUET_COMPRESSION)
            
            logger.info(f"Campo {field} procesado - {len(df)} objetos")
            return df

        except Exception as e:
            logger.error(f"Error en campo {field} (Intento {attempt+1}): {str(e)}")
            time.sleep(RETRY_DELAY * (1 + attempt))
    
    logger.error(f"Campo {field} falló después de {MAX_RETRIES} intentos")
    failed_fields.append(field)
    return None

def combine_parquets(output_dir):
    """Combina todos los archivos Parquet de forma eficiente"""
    parquet_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
    
    if not parquet_files:
        logger.warning("No se encontraron archivos Parquet para combinar")
        return pd.DataFrame()

    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(os.path.join(output_dir, file))
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error leyendo {file}: {str(e)}")
    
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def main(test_mode=False):
    """Función principal optimizada"""
    start_time = time.time()
    output_dir = "splus_results_all_disk_mc"
    os.makedirs(output_dir, exist_ok=True)
    
    failed_fields = ThreadSafeList()
    processed_data = []
    
    fields = pd.read_csv("iDR5_pointings.csv")
    if test_mode:
        fields = fields.sample(2)
        logger.info("Modo prueba: 2 campos aleatorios")

    with create_connection() as conn:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    process_field,
                    conn,
                    field,
                    output_dir,
                    failed_fields
                ): field for field in fields["iDR5_Field_Name"]
            }
            
            for future in as_completed(futures):
                field = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        processed_data.append(result)
                except Exception as e:
                    logger.error(f"Error crítico en {field}: {str(e)}")
                    failed_fields.append(field)

    # Procesamiento final
    final_df = pd.concat(processed_data, ignore_index=True) if processed_data else pd.DataFrame()
    
    if not final_df.empty:
        try:
            astropy_table = Table.from_pandas(final_df)
            astropy_table.meta.clear()
            
            output_file = f"splus_data_iDR5_disk_full_MC_{time.strftime('%Y%m%d%H%M')}.fits"
            astropy_table.write(output_file, format='fits', overwrite=True)
            
            logger.info(f"Archivo FITS creado: {output_file} ({len(final_df)} objetos)")
            logger.info(f"Tamaño: {os.path.getsize(output_file)/1024**2:.2f} MB")
        except Exception as e:
            logger.error(f"Error creando FITS: {str(e)}")
    else:
        logger.warning("No se encontraron datos para generar el archivo FITS")

    # Guardar campos fallidos
    if failed_fields._list:
        pd.DataFrame(list(failed_fields), columns=["Field"]).to_csv("failed_fields.csv", index=False)
        logger.info(f"Campos fallidos: {len(failed_fields._list)}")

    logger.info(f"Tiempo total: {(time.time()-start_time)/60:.1f} minutos")

if __name__ == "__main__":
    main(test_mode=False)
