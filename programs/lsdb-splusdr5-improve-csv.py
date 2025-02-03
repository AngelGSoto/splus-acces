import argparse
import splusdata
import lsdb
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import urllib
import time
import os
import logging
from http.client import IncompleteRead  # Importar IncompleteRead

# Configurar el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def retry_request(func, retries=5, delay=10, *args, **kwargs):
    """Retry a request function with specified retries and delay."""
    for attempt in range(retries):
        try:
            logger.info(f"Attempt {attempt + 1} to call {func.__name__} with args: {args} and kwargs: {kwargs}")
            return func(*args, **kwargs)
        except (urllib.error.URLError, ConnectionRefusedError, TimeoutError, IncompleteRead) as e:
            if attempt == retries - 1:
                raise
            logger.warning(f"Retrying due to error: {e}. Attempt {attempt + 1}/{retries}")
            time.sleep(delay)

def load_data_with_retry(conn_lsdb, idr5_link, filters=[]):
    """Load data with retry mechanism."""
    logger.info(f"Loading data from {idr5_link} with filters {filters}")
    try:
        data = retry_request(lsdb.read_hipscat, 5, 10, idr5_link, storage_options=dict(headers=conn_lsdb.headers), filters=filters)
        if data is None:
            logger.error(f"No data returned from {idr5_link} with filters {filters}")
        return data
    except Exception as e:
        logger.error(f"Failed to load data from {idr5_link} with filters {filters}: {e}")
        return None

def catalog_to_dataframe(catalog):
    """Convert a Catalog object to a pandas DataFrame."""
    if catalog is None:
        logger.error("Received None catalog.")
        return pd.DataFrame()
    
    # Verificar si el catálogo tiene el método to_pandas
    try:
        return catalog.to_pandas()
    except AttributeError:
        logger.warning("Catalog object has no method to_pandas. Converting to list of dicts.")
        # Convertir a lista de diccionarios
        data = [dict(row) for row in catalog]
        return pd.DataFrame(data)

def has_data(catalog):
    """Check if a Catalog object has data."""
    try:
        return bool(catalog)
    except Exception as e:
        logger.error(f"Error checking if catalog has data: {e}")
        return False

def process_magnitude_range(conn_lsdb, idr5_links, idr5_margin, mag_min, mag_max, step_size, sqg):
    """Process data for a given magnitude range and return combined results."""
    current_min = mag_min
    all_results = []

    while current_min < mag_max:
        current_max = min(current_min + step_size, mag_max)
        filters = [
            ("r_PStotal", ">", current_min),
            ("r_PStotal", "<=", current_max),
            ("e_r_PStotal", "<=", 0.3),
            ("e_g_PStotal", "<=", 0.3),
            ("e_i_PStotal", "<=", 0.3),
            ("e_z_PStotal", "<=", 0.3),
            ("e_J0515_PStotal", "<=", 0.3),
            ("e_J0660_PStotal", "<=", 0.3),
            ("e_J0395_PStotal", "<=", 0.3),
            ("e_J0410_PStotal", "<=", 0.3),
            ("e_J0430_PStotal", "<=", 0.3),
            ("e_J0861_PStotal", "<=", 0.3)
        ]
        try:
            logger.info(f"Reading dual data for r_PStotal between {current_min} and {current_max}")
            dual = lsdb.read_hipscat(
                idr5_links[0],
                margin_cache=idr5_margin,
                storage_options=dict(headers=conn_lsdb.headers),
                columns=[
                    "Field", "ID", "RA", "DEC", "X", "Y", "A", "B", "ELLIPTICITY",
                    "ELONGATION", "FWHM", "KRON_RADIUS", "PETRO_RADIUS", "ISOarea",
                    "MU_MAX_r", "MU_MAX_J0660", "MU_MAX_i", "s2n_DET_PStotal",
                    "s2n_g_PStotal", "s2n_J0515_PStotal", "s2n_r_PStotal",
                    "s2n_J0660_PStotal", "s2n_i_PStotal", "SEX_FLAGS_DET",
                    "SEX_FLAGS_u", "SEX_FLAGS_J0378", "SEX_FLAGS_J0395",
                    "SEX_FLAGS_J0410", "SEX_FLAGS_J0430", "SEX_FLAGS_g",
                    "SEX_FLAGS_J0515", "SEX_FLAGS_r", "SEX_FLAGS_J0660",
                    "SEX_FLAGS_i", "SEX_FLAGS_J0861", "SEX_FLAGS_z",
                    "CLASS_STAR", "r_PStotal", "e_r_PStotal", "g_PStotal",
                    "e_g_PStotal", "i_PStotal", "e_i_PStotal", "u_PStotal",
                    "e_u_PStotal", "z_PStotal", "e_z_PStotal",
                    "J0378_PStotal", "e_J0378_PStotal", "J0395_PStotal",
                    "e_J0395_PStotal", "J0410_PStotal", "e_J0410_PStotal",
                    "J0430_PStotal", "e_J0430_PStotal", "J0515_PStotal",
                    "e_J0515_PStotal", "J0660_PStotal", "e_J0660_PStotal",
                    "J0861_PStotal", "e_J0861_PStotal",
                ],
                filters=filters
            )
            # Verificar si dual es un objeto Catalog con datos
            if not has_data(dual):
                logger.error(f"Failed to load dual data for r_PStotal between {current_min} and {current_max}: no data in catalog.")
                current_min = current_max
                continue
            logger.info(f"Datos de dual cargados correctamente para r_PStotal entre {current_min} y {current_max}.")
            
            # Convertir a pandas DataFrame
            dual_df = catalog_to_dataframe(dual)
            if dual_df.empty:
                logger.error(f"Failed to convert dual data to DataFrame for r_PStotal between {current_min} and {current_max}")
                current_min = current_max
                continue
            logger.info(f"Datos de dual convertidos a DataFrame de pandas para r_PStotal entre {current_min} y {current_max}.")
            
            # Realizar crossmatch con sqg
            logger.info(f"Performing crossmatch for r_PStotal between {current_min} and {current_max}")
            dual_sqg = sqg.crossmatch(dual, radius_arcsec=1)
            dual_sqg_df = catalog_to_dataframe(dual_sqg)
            if dual_sqg_df.empty:
                logger.error(f"Failed to convert crossmatch data to DataFrame for r_PStotal between {current_min} and {current_max}")
                current_min = current_max
                continue
            logger.info(f"Crossmatch data converted to DataFrame de pandas for r_PStotal between {current_min} and {current_max}.")
            
            all_results.append(dual_sqg_df)  # Añadir a la lista
        except Exception as e:
            logger.error(f"Error al leer datos de dual para r_PStotal entre {current_min} y {current_max}: {e}")

        current_min = current_max

    if not all_results:
        logger.error("No results to concatenate. Returning empty DataFrame.")
        return pd.DataFrame()
    
    return pd.concat(all_results, ignore_index=True)  # Concatenar todos los DataFrames de pandas

def main(username, password):
    logger.info("Authenticating to splus.cloud")
    conn_lsdb = splusdata.Core(username=username, password=password)
    logger.info("Conexión LSDB establecida")

    try:
        logger.info("Getting iDR5 links")
        idr5_links = retry_request(splusdata.get_hipscats, 5, 10, "idr5/dual", headers=conn_lsdb.headers)[0]
        logger.info(f"Enlaces iDR5 obtenidos: {idr5_links}")
    except Exception as e:
        logger.error(f"Error al obtener enlaces de iDR5: {e}")
        return

    try:
        logger.info("Reading iDR5 margin data")
        idr5_margin = load_data_with_retry(conn_lsdb, idr5_links[1], [])
        if idr5_margin is None:
            logger.error("Failed to load iDR5 margin data")
            return
        logger.info(f"Columnas de idr5_margin: {idr5_margin.columns}")
    except Exception as e:
        logger.error(f"Error al leer datos de idr5_margin: {e}")
        return

    try:
        logger.info("Getting iDR5 links for SQG")
        idr5_sqg = retry_request(splusdata.get_hipscats, 5, 10, "idr5/sqg", headers=conn_lsdb.headers)[0]
        sqg_margin = load_data_with_retry(conn_lsdb, idr5_sqg[1], [])
        if sqg_margin is None:
            logger.error("Failed to load SQG margin data")
            return
        logger.info(f"Columnas de sqg_margin: {sqg_margin.columns}")
    except Exception as e:
        logger.error(f"Error al obtener enlaces de iDR5 para SQG: {e}")
        return

    try:
        logger.info("Reading SQG data")
        sqg = lsdb.read_hipscat(
            idr5_sqg[0],
            margin_cache=sqg_margin,
            storage_options=dict(headers=conn_lsdb.headers),
            columns=["RA", "DEC", "CLASS", "PROB_QSO", "PROB_STAR", "PROB_GAL", "Plx"],
            filters=[("CLASS", "=", 1)]
        )
        if sqg is None:
            logger.error("Failed to load SQG data")
            return
        logger.info("Datos de sqg cargados correctamente.")
    except Exception as e:
        logger.error(f"Error al leer datos de sqg: {e}")
        return

    # Definir los rangos de magnitud
    magnitude_ranges = [(0, 18, 'dual_pstotal_sqg_star_18r.csv'), 
                        (18, 20, 'dual_pstotal_sqg_star_18r20.csv'), 
                        (20, 25, 'dual_pstotal_sqg_star_r20.csv')]
    step_size = 0.5  # Tamaño del paso dentro de cada rango

    for mag_min, mag_max, output_file in magnitude_ranges:
        logger.info(f"Processing magnitude range {mag_min} to {mag_max}")
        combined_results = process_magnitude_range(conn_lsdb, idr5_links, idr5_margin, mag_min, mag_max, step_size, sqg)
        
        if combined_results.empty:
            logger.error(f"No data to save for magnitude range {mag_min} to {mag_max}. Skipping file save.")
            continue
        
        # Convertir el resultado combinado a DataFrame de Dask
        dual_sqg_ddf = dd.from_pandas(combined_results, npartitions=100)
        logger.info(f"Converted results to Dask DataFrame for magnitude range {mag_min} to {mag_max}")

        logger.info(f"Saving results to {output_file}")
        dual_sqg_ddf.to_csv(f'Data_iDR5_raimundo/{output_file}', index=False)
        logger.info(f"Results saved to {output_file}")

    logger.info("Todos los datos del resultado del crossmatch se han guardado en los archivos correspondientes.")

    # Notas Importantes y Siguiente Paso
    logger.info("\n### Notas Importantes:\n")
    logger.info("**Advertencia sobre el Tamaño del Gráfico**: La advertencia indica que se envió un gráfico grande de tamaño 71.27 MiB. Esto puede causar cierta ralentización. Para mejorar el rendimiento en el futuro, considera cargar los datos directamente con Dask o usar futuros u objetos retrasados para embebir los datos en el gráfico sin repetición.")
    logger.info("  - **Enlace a Mejores Prácticas de Dask**: [Best Practices](https://docs.dask.org/en/stable/best-practices.html#load-data-with-dask)\n")
    logger.info("### Siguiente Paso:\n")
    logger.info("**Verificar los Resultados**: Puedes abrir y verificar los archivos generados para asegurarte de que los datos se guardaron correctamente.")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run LSDB script with SPLUS authentication")
    parser.add_argument("--username", required=True, help="splus.cloud username")
    parser.add_argument("--password", required=True, help="splus.cloud password")
    args = parser.parse_args()

    # Start Dask client with the desired specs
    logger.info("Starting Dask client")
    try:
        client = Client(n_workers=4, memory_limit="3.5GB", timeout=60)  # Adjusted workers and memory limit
        logger.info("Dask client started successfully")
        logger.info(client)
    except Exception as e:
        logger.error(f"Error starting Dask client: {e}")
    main(args.username, args.password)
