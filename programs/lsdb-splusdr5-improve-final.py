import splusdata
import lsdb
import pandas as pd
from getpass import getpass
from dask.distributed import Client
import urllib
import time
from dask import config, dataframe as dd

def retry_request(func, retries=10, delay=20, *args, **kwargs):
    """Retry a request function with specified retries and delay."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except urllib.error.URLError as e:
            if attempt == retries - 1:
                raise
            print(f"Retrying due to error: {e}. Attempt {attempt + 1}/{retries}")
            time.sleep(delay)

def inspect_catalog(catalog):
    # Check catalog type and attributes
    print("Catalog type:", type(catalog))
    print("Available attributes and methods:")
    print(dir(catalog))

    # Try to print the first few rows (if possible)
    try:
        print("First few rows of catalog:")
        print(catalog.head())  # This might work based on available methods
    except Exception as e:
        print(f"Error previewing catalog: {e}")

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
        # Check for existing Dask client and shut it down
        try:
            existing_client = Client()
            existing_client.shutdown()
        except Exception as e:
            print("No existing Dask client found or error shutting down existing client:", e)

        # Configure Dask Client with increased timeout and more workers
        config.set({
            "distributed.comm.timeouts.connect": "300s",
            "distributed.comm.timeouts.tcp": "300s"
        })
        client = Client(n_workers=8, threads_per_worker=2, memory_limit="4GB")
        print(client)
    except Exception as e:
        print("Error al configurar Dask Client:", e)
        return

    try:
        # Read iDR5 margin data with retry
        idr5_margin = retry_request(lsdb.read_hipscat, 10, 20, idr5_links[1], storage_options=dict(headers=conn_lsdb.headers))
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
                ("r_PStotal", "<=", 13),
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
        sqg_margin = retry_request(lsdb.read_hipscat, 10, 20, idr5_sqg[1], storage_options=dict(headers=conn_lsdb.headers))
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
        # Inspect the type of dual_sqg
        print(f"Tipo de 'dual_sqg': {type(dual_sqg)}")
        inspect_catalog(dual_sqg)  # Inspect the catalog

        # Convert the Catalog to a Dask DataFrame
        dual_sqg_ddf = dd.from_delayed(dual_sqg.to_delayed())

        # Convert Dask DataFrame to pandas and concatenate in memory
        pandas_dfs = []
        for partition in dual_sqg_ddf.to_delayed():
            pandas_dfs.append(partition.compute())

        combined_df = pd.concat(pandas_dfs, ignore_index=True)

        # Save the combined DataFrame to a single CSV file
        combined_df.to_csv('dual_sqg_full_test.csv', index=False)

        print("Todos los datos del resultado del crossmatch se han guardado en 'dual_sqg_full.csv'")
    except Exception as e:
        print("Error durante el guardado de todos los datos del crossmatch:", e)

if __name__ == "__main__":
    main()
