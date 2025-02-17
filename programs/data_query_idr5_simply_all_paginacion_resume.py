import splusdata
import os
import logging
import time
import re
import numpy as np
from getpass import getpass
from contextlib import contextmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from astropy.table import Table
from astropy.io import fits
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd

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

# Configuración avanzada
MAX_RETRIES = 5
RETRY_DELAY = 15
MAX_WORKERS = 8
PARQUET_COMPRESSION = 'snappy'
OBJECT_LIMIT = 800000
MAX_CHUNK_DIVISIONS = 6
MIN_CHUNK_SIZE = 0.1
GLOBAL_TIMEOUT = 3600  # 1 hora por campo
BACKOFF_FACTOR = 1.5

class ThreadSafeList:
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
    session = requests.Session()
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[500, 502, 503, 504, 408, 429],
        allowed_methods=['HEAD', 'GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'TRACE']
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=MAX_WORKERS * 2,
        pool_maxsize=MAX_WORKERS * 2
    )
    
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
    if not isinstance(field, str) or not re.match(r'^[a-zA-Z0-9_-]+$', field):
        raise ValueError(f"Nombre de campo inválido: {field}")
    return field

def get_field_bounds(conn, field):
    try:
        query = f'''
        SELECT 
            MIN(RA) as ra_min,
            MAX(RA) as ra_max,
            MIN("DEC") as dec_min,
            MAX("DEC") as dec_max
        FROM "idr5"."idr5_psf"
        WHERE Field = '{sanitize_field(field)}'
        '''
        result = conn.query(query)
        
        if not result or len(result) == 0:
            logger.error(f"Consulta vacía para {field}")
            return None
            
        return {
            'ra_min': result['ra_min'][0],
            'ra_max': result['ra_max'][0],
            'dec_min': result['dec_min'][0],
            'dec_max': result['dec_max'][0]
        }
    except Exception as e:
        logger.error(f"Error en get_field_bounds: {str(e)}")
        return None

def adaptive_chunking(conn, field, ra_min, ra_max, dec_min, dec_max, depth=0):
    try:
        query_count = f'''
        SELECT COUNT(*) AS "count"
        FROM "idr5"."idr5_psf"
        WHERE 
            Field = '{sanitize_field(field)}'
            AND RA BETWEEN {ra_min} AND {ra_max}
            AND "DEC" BETWEEN {dec_min} AND {dec_max}
        '''
        
        result = conn.query(query_count)
        
        if not result or len(result) == 0:
            logger.error(f"Error en conteo para {field}")
            return []
            
        count = result['count'][0]
        
        if count <= OBJECT_LIMIT or depth >= MAX_CHUNK_DIVISIONS:
            return [(ra_min, ra_max, dec_min, dec_max)]
        
        if (ra_max - ra_min < MIN_CHUNK_SIZE) or (dec_max - dec_min < MIN_CHUNK_SIZE):
            logger.warning(f"Chunk mínimo alcanzado con {count} objetos")
            return [(ra_min, ra_max, dec_min, dec_max)]
        
        ra_center = (ra_min + ra_max) / 2
        dec_center = (dec_min + dec_max) / 2
        
        chunks = []
        chunks += adaptive_chunking(conn, field, ra_min, ra_center, dec_min, dec_center, depth+1)
        chunks += adaptive_chunking(conn, field, ra_min, ra_center, dec_center, dec_max, depth+1)
        chunks += adaptive_chunking(conn, field, ra_center, ra_max, dec_min, dec_center, depth+1)
        chunks += adaptive_chunking(conn, field, ra_center, ra_max, dec_center, dec_max, depth+1)
        
        return chunks
    
    except Exception as e:
        logger.error(f"Error en chunking: {str(e)}")
        return []

def build_query(field, ra_min, ra_max, dec_min, dec_max):
    safe_field = sanitize_field(field)
    return f'''
    SELECT 
        psf.ID, psf.RA, psf."DEC",
        psf.g_psf, psf.e_g_psf
    FROM "idr5"."idr5_psf" AS psf
    WHERE 
        psf.Field = '{safe_field}'
        AND psf.RA BETWEEN {ra_min} AND {ra_max}
        AND psf."DEC" BETWEEN {dec_min} AND {dec_max}
    '''

def execute_query(conn, query, field, chunk_id):
    for attempt in range(MAX_RETRIES):
        try:
            result = conn.query(query)
            
            if not result or len(result) == 0:
                logger.warning(f"Consulta vacía en chunk {chunk_id}")
                return None
                
            df = result.to_pandas()
            
            if len(df) > OBJECT_LIMIT:
                logger.error(f"Chunk {chunk_id} excede límite con {len(df)} objetos")
                return None
                
            type_map = {
                'RA': 'float32',
                'DEC': 'float32',
                'g_psf': 'float32',
                'e_g_psf': 'float32'
            }
            return df.astype({k: v for k, v in type_map.items() if k in df})
            
        except Exception as e:
            logger.error(f"Error en chunk {chunk_id} (Intento {attempt+1}): {str(e)}")
            time.sleep(RETRY_DELAY * (BACKOFF_FACTOR ** attempt))
    
    logger.error(f"Chunk {chunk_id} falló después de {MAX_RETRIES} intentos")
    return None

def process_field(conn, field, output_dir, failed_chunks):
    logger.info(f"Iniciando procesamiento de: {field}")
    start_time = time.time()
    total_objects = 0
    
    try:
        bounds = get_field_bounds(conn, field)
        if not bounds:
            logger.error(f"Límites no obtenidos para {field}")
            failed_chunks.append(field)
            return None
            
        chunks = adaptive_chunking(
            conn, field,
            bounds['ra_min'], bounds['ra_max'],
            bounds['dec_min'], bounds['dec_max']
        )
        
        if not chunks:
            logger.error(f"No se generaron chunks para {field}")
            failed_chunks.append(field)
            return None
            
        logger.info(f"Campo {field} dividido en {len(chunks)} chunks")
        
        dfs = []
        for i, (ra_min, ra_max, dec_min, dec_max) in enumerate(chunks):
            if time.time() - start_time > GLOBAL_TIMEOUT:
                logger.error(f"Timeout global en campo {field}")
                failed_chunks.append(field)
                return None
                
            chunk_id = f"{field}_{ra_min:.2f}-{ra_max:.2f}_{dec_min:.2f}-{dec_max:.2f}"
            logger.info(f"Procesando chunk {i+1}/{len(chunks)}: {chunk_id}")
            
            query = build_query(field, ra_min, ra_max, dec_min, dec_max)
            df_chunk = execute_query(conn, query, field, chunk_id)
            
            if df_chunk is not None:
                output_file = os.path.join(output_dir, f"{chunk_id}.parquet")
                df_chunk.to_parquet(output_file, compression=PARQUET_COMPRESSION)
                dfs.append(df_chunk)
                total_objects += len(df_chunk)
            else:
                failed_chunks.append(chunk_id)
        
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            output_file = os.path.join(output_dir, f"{field}_FULL.parquet")
            final_df.to_parquet(output_file, compression=PARQUET_COMPRESSION)
            logger.info(f"→→→ Campo {field} completado con {total_objects:,} objetos ←←←")
            return final_df
            
        return None
        
    except Exception as e:
        logger.error(f"Error crítico en campo {field}: {str(e)}")
        failed_chunks.append(field)
        return None

def validate_and_combine(conn, output_dir):
    try:
        parquet_files = [f for f in os.listdir(output_dir) if f.endswith('_FULL.parquet')]
        
        if not parquet_files:
            logger.warning("No se encontraron archivos completos")
            return pd.DataFrame()

        total_objects = 0
        field_counts = {}
        
        for file in parquet_files:
            try:
                df = pd.read_parquet(os.path.join(output_dir, file))
                field_name = file.split('_FULL')[0]
                count = len(df)
                field_counts[field_name] = count
                total_objects += count
            except Exception as e:
                logger.error(f"Error leyendo {file}: {str(e)}")

        # Validación contra la base de datos
        for field, count in field_counts.items():
            try:
                query = f'''SELECT COUNT(*) AS "count" 
                          FROM "idr5"."idr5_psf" 
                          WHERE Field = '{sanitize_field(field)}' '''
                result = conn.query(query)
                
                if not result or len(result) == 0:
                    logger.error(f"Error validando {field}")
                    continue
                    
                db_count = result['count'][0]
                
                status = "✅ OK" if count == db_count else f"❌ DISCREPANCIA ({count - db_count:+})"
                logger.info(f"{status} - {field}: Local {count:,} vs BD {db_count:,}")
                
            except Exception as e:
                logger.error(f"Error validando {field}: {str(e)}")

        logger.info(f"\n{'='*60}")
        logger.info(f"TOTAL GENERAL DE OBJETOS: {total_objects:,}")
        logger.info(f"{'='*60}\n")
        
        return pd.concat([pd.read_parquet(os.path.join(output_dir, f)) 
                        for f in parquet_files], ignore_index=True)
    
    except Exception as e:
        logger.error(f"Error en validación: {str(e)}")
        return pd.DataFrame()

def main(test_mode=False):
    start_time = time.time()
    output_dir = "splus_results_all_MC_disk"
    os.makedirs(output_dir, exist_ok=True)
    
    failed_chunks = ThreadSafeList()
    
    try:
        # Cargar y filtrar campos
        fields = pd.read_csv("iDR5_pointings.csv")
        target_fields = fields[
            fields["iDR5_Field_Name"].str.startswith("MC", na=False)
            #fields["iDR5_Field_Name"].str.contains(r'[-_][bd]\d{2,}', regex=True, na=False)
        ]

        # Detectar campos ya procesados
        existing_fields = set([
            f.split("_FULL")[0] 
            for f in os.listdir(output_dir) 
            if f.endswith("_FULL.parquet")
        ])
        
        # Filtrar campos pendientes
        target_fields = target_fields[
            ~target_fields["iDR5_Field_Name"].isin(existing_fields)
        ]
        
        if len(target_fields) == 0:
            logger.info("¡Todos los campos ya han sido procesados!")
            return

        if test_mode:
            target_fields = target_fields.sample(2)
            logger.info("Modo prueba: 2 campos aleatorios")

        logger.info(f"Campos pendientes: {len(target_fields)}")
        
        with create_connection() as conn:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        process_field,
                        conn,
                        field,
                        output_dir,
                        failed_chunks
                    ): field for field in target_fields["iDR5_Field_Name"]
                }
                
                for future in as_completed(futures):
                    field = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error crítico en {field}: {str(e)}")
                        failed_chunks.append(field)

            # Validación final
            validate_and_combine(conn, output_dir)

    except Exception as e:
        logger.error(f"Error en main: {str(e)}")

    # Guardar chunks fallidos
    if failed_chunks._list:
        pd.DataFrame(list(failed_chunks), columns=["Chunk"]).to_csv("failed_chunks.csv", index=False)
        logger.info(f"Chunks fallidos: {len(failed_chunks._list)}")

    logger.info(f"Tiempo total: {(time.time()-start_time)/60:.1f} minutos")

if __name__ == "__main__":
    main(test_mode=False)
