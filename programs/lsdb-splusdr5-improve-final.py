import splusdata
import lsdb
import pandas as pd
from getpass import getpass
import urllib
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def process_partition(partition, index, temp_dir, retries=5, delay=10):
    """Process and save a partition of the DataFrame with retries."""
    for attempt in range(retries):
        try:
            partition_file = os.path.join(temp_dir, f'partition_{index}.csv')
            partition_df = partition.compute()
            partition_df.to_csv(partition_file, index=False)
            print(f"Partition {index} saved to {partition_file}")
            return
        except (urllib.error.URLError, ConnectionRefusedError, TimeoutError) as e:
            if attempt == retries - 1:
                print(f"Failed to process partition {index} after {retries} attempts.")
                raise
            print(f"Retrying partition {index} due to error: {e}. Attempt {attempt + 1}/{retries}")
            time.sleep(delay)

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
        # Define temporary directory for intermediate CSV files
        temp_dir = "temp_csvs"
        os.makedirs(temp_dir, exist_ok=True)

        # Initialize partition index
        partition_index = 0

        # Use ThreadPoolExecutor to process partitions in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_partition, partition, i, temp_dir) for i, partition in enumerate(dual_sqg.partitions)]

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing partition: {e}")

        # Combine the temporary CSV files into a single DataFrame
        all_files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.startswith('partition_')]
        combined_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

        # Save the combined DataFrame to a single CSV file
        combined_df.to_csv('dual_sqg_full.csv', index=False)

        # Cleanup temporary directory
        for f in all_files:
            os.remove(f)
        os.rmdir(temp_dir)

        print("Todos los datos del resultado del crossmatch se han guardado en 'dual_sqg_full.csv'")
    except Exception as e:
        print("Error durante el guardado de todos los datos del crossmatch:", e)

if __name__ == "__main__":
    main()
