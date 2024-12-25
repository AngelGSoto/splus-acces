import splusdata
import lsdb
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from dask.distributed import Client
from getpass import getpass
import urllib
import time
import os

def retry_request(func, retries=10, delay=20, *args, **kwargs):
    """Retry a request function with specified retries and delay."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except (urllib.error.URLError, ConnectionRefusedError, TimeoutError) as e:
            if attempt == retries - 1:
                raise
            print(f"Retrying due to error: {e}. Attempt {attempt + 1}/{retries}")
            time.sleep(delay)

def load_data_with_retry(conn_lsdb, idr5_link):
    """Load data with retry mechanism."""
    return retry_request(lsdb.read_hipscat, 10, 20, idr5_link, storage_options=dict(headers=conn_lsdb.headers))

def main():
    # Authenticate to splus.cloud
    username = input("splus.cloud username: ")
    password = getpass("splus.cloud password: ")
    conn_lsdb = splusdata.Core(username=username, password=password)

    print("Conexión LSDB establecida:", conn_lsdb)

    try:
        # Get iDR5 links with retry
        idr5_links = retry_request(splusdata.get_hipscats, 10, 20, "idr5/dual", headers=conn_lsdb.headers)[0]
        print("Enlaces iDR5 obtenidos:", idr5_links)
    except Exception as e:
        print("Error al obtener enlaces de iDR5:", e)
        return

    try:
        # Read iDR5 margin data with retry
        idr5_margin = load_data_with_retry(conn_lsdb, idr5_links[1])
        print("Columnas de idr5_margin:", idr5_margin.columns)
    except Exception as e:
        print("Error al leer datos de idr5_margin:", e)
        return

    try:
        # Read dual data with specific filters and columns
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
            filters=[
                ("r_PStotal", "<=", 18),
                ("e_r_PStotal", "<=", 0.3),
                ("e_g_PStotal", "<=", 0.3),
                ("e_i_PStotal", "<=", 0.3),
                ("e_z_PStotal", "<=", 0.3),
                ("e_J0515_PStotal", "<=", 0.3),
                ("e_J0660_PStotal", "<=", 0.3),
                ("e_J0395_PStotal", "<=", 0.3),
                ("e_J0410_PStotal", "<=", 0.3),
                ("e_J0430_PStotal", "<=", 0.3),
                ("e_J0861_PStotal", "<=", 0.3),
                # Excluded e_u_PStotal and e_J0378_PStotal
            ]
        )
        print("Datos de dual cargados correctamente.")
    except Exception as e:
        print("Error al leer datos de dual:", e)
        return

    try:
        # Get iDR5 links for SQG with retry
        idr5_sqg = retry_request(splusdata.get_hipscats, 10, 20, "idr5/sqg", headers=conn_lsdb.headers)[0]
        sqg_margin = load_data_with_retry(conn_lsdb, idr5_sqg[1])
        print("Columnas de sqg_margin:", sqg_margin.columns)
    except Exception as e:
        print("Error al obtener enlaces de iDR5 para SQG:", e)
        return

    try:
        # Read SQG data with specific filters and columns
        sqg = lsdb.read_hipscat(
            idr5_sqg[0],
            margin_cache=sqg_margin,
            storage_options=dict(headers=conn_lsdb.headers),
            columns=["RA", "DEC", "CLASS", "PROB_QSO", "PROB_STAR", "PROB_GAL", "Plx"],
            filters=[("CLASS", "=", 1)]
        )
        print("Datos de sqg cargados correctamente.")
    except Exception as e:
        print("Error al leer datos de sqg:", e)
        return

    try:
        # Perform crossmatch
        dual_sqg = sqg.crossmatch(dual, radius_arcsec=1)
        print("Crossmatch completado.")
    except Exception as e:
        print("Error durante el crossmatch:", e)
        return

    try:
        # Convert the crossmatch result to a Dask DataFrame
        dual_sqg_ddf = dd.from_pandas(dual_sqg.compute(), npartitions=100)

        # Save each partition as a separate CSV file
        temp_dir = "temp_csvs"
        os.makedirs(temp_dir, exist_ok=True)
        dual_sqg_ddf.to_csv(os.path.join(temp_dir, 'partition_*.csv'), index=False)

        # Combine the CSV files into a single DataFrame
        combined_df = dd.read_csv(os.path.join(temp_dir, 'partition_*.csv'))

        # Save the combined DataFrame to a single CSV file
        combined_df.compute().to_csv('Data_iDR5_raimundo/dual_pstotal_sqg_star_18r.csv', index=False)

        # Cleanup temporary directory
        for f in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, f))
        os.rmdir(temp_dir)

        print("Todos los datos del resultado del crossmatch se han guardado en 'Data_iDR5_raimundo/dual_pstotal_sqg_star_full.csv'")

        # Notas Importantes y Siguiente Paso
        print("\n### Notas Importantes:\n")
        print("**Advertencia sobre el Tamaño del Gráfico**: La advertencia indica que se envió un gráfico grande de tamaño 71.27 MiB. Esto puede causar cierta ralentización. Para mejorar el rendimiento en el futuro, considera cargar los datos directamente con Dask o usar futuros u objetos retrasados para embebir los datos en el gráfico sin repetición.")
        print("  - **Enlace a Mejores Prácticas de Dask**: [Best Practices](https://docs.dask.org/en/stable/best-practices.html#load-data-with-dask)\n")
        print("### Siguiente Paso:\n")
        print("**Verificar los Resultados**: Puedes abrir y verificar el archivo 'dual_sqg_full.csv' para asegurarte de que los datos se guardaron correctamente.")
        
    except Exception as e:
        print("Error durante el guardado de todos los datos del crossmatch:", e)

if __name__ == "__main__":
    # Start Dask client with the desired specs
    client = Client(n_workers=6, memory_limit="2GB", timeout=60)  # Increased timeout
    print(client)
    main()
