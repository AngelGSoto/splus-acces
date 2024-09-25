import pandas as pd
import splusdata
import lsdb
from getpass import getpass

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
    # Load your catalog with planetary nebulae
    local_catalog = pd.read_csv("GUVcat_AISxSDSS_HSmaster.csv")
    
    # Connect to S-PLUS
    username = input("S-PLUS Username: ")
    password = getpass("S-PLUS Password: ")
    
    try:
        conn = splusdata.connect(username, password)
    except Exception as e:
        print(f"Error connecting to S-PLUS: {e}")
        return
    
    # Connect to LSDB for iDR5
    conn_lsdb = splusdata.Core()

    # Get links for iDR5 dual and psf data
    idr5_links = splusdata.get_hipscats("idr5/dual", headers=conn_lsdb.headers)[0]
    idr5_psf = splusdata.get_hipscats("idr5/psf", headers=conn_lsdb.headers)[0]

    # Load the dual data
    try:
        idr5_margin = lsdb.read_hipscat(idr5_links[1], storage_options=dict(headers=conn_lsdb.headers))
        dual = lsdb.read_hipscat(
            idr5_links[0],
            margin_cache=idr5_margin,
            storage_options=dict(headers=conn_lsdb.headers),
            columns=["ID", "RA", "DEC", "Field", "FWHM", "KRON_RADIUS"]
        )

        # Inspect catalog object to understand its structure
        inspect_catalog(dual)

        # Convert the catalog using compute() and assign to DataFrame
        dual_df = dual.compute()
        print(f"Dual catalog converted to DataFrame: {dual_df.head()}")

    except Exception as e:
        print(f"Error loading or converting dual data: {e}")
        return

    # Load the psf data
    try:
        psf_margin = lsdb.read_hipscat(idr5_psf[1], storage_options=dict(headers=conn_lsdb.headers))
        psf = lsdb.read_hipscat(
            idr5_psf[0],
            margin_cache=psf_margin,
            storage_options=dict(headers=conn_lsdb.headers),
            columns=["ID", "RA", "DEC"]
        )
    except Exception as e:
        print(f"Error loading psf data: {e}")
        return

    # Check if either catalog is empty
    if len(dual_df) == 0 or len(psf) == 0:
        print("One of the catalogs is empty. Check your data.")
        return

    # Crossmatch psf with dual data (2 arcsecond search radius)
    try:
        dual_psf = psf.crossmatch(dual, radius_arcsec=2).compute()
    except Exception as e:
        print(f"Error during crossmatch between dual and psf: {e}")
        return

    # Convert the dual_psf crossmatch result to a DataFrame
    try:
        dual_psf_df = pd.DataFrame(dual_psf)
        print(f"Crossmatch dual_psf result: {dual_psf_df.head()}")
    except Exception as e:
        print(f"Error converting crossmatch result to DataFrame: {e}")
        return

    # Ensure that the RA and DEC columns in your local catalog are correctly named and not empty
    if 'GALEX_RA' not in local_catalog.columns or 'GALEX_DEC' not in local_catalog.columns:
        print("Error: RA/DEC columns not found in the local catalog.")
        return

    if local_catalog['GALEX_RA'].isnull().any() or local_catalog['GALEX_DEC'].isnull().any():
        print("Error: RA/DEC columns contain null values in the local catalog.")
        return

    # Load the local catalog into LSDB format
    try:
        local_hips = lsdb.from_dataframe(local_catalog, ra_column="GALEX_RA", dec_column="GALEX_DEC", margin_threshold=3600)
    except Exception as e:
        print(f"Error loading local catalog into LSDB format: {e}")
        return

    # Perform crossmatch with the dual_psf catalog using a 2 arcsecond search radius
    try:
        matched_table = local_hips.crossmatch(dual_psf, radius_arcsec=2).compute()
    except Exception as e:
        print(f"Error during crossmatch between local catalog and dual_psf: {e}")
        return

    # Convert the result back to a Pandas DataFrame
    try:
        matched_table_df = pd.DataFrame(matched_table)
        print(f"Crossmatched data preview: {matched_table_df.head()}")
    except Exception as e:
        print(f"Error converting final crossmatch result to DataFrame: {e}")
        return

    # Save the crossmatched results to a CSV file
    try:
        matched_table_df.to_csv("GUVcat_AISxSDSS_HSmaster_splus_lsbd.csv", index=False)
        print("Crossmatch complete! Results saved to GUVcat_AISxSDSS_HSmaster_splus.csv")
    except Exception as e:
        print(f"Error saving crossmatch results to CSV: {e}")

if __name__ == "__main__":
    main()
