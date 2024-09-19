import pandas as pd
import splusdata
from astropy.coordinates import SkyCoord
import astropy.units as u
from getpass import getpass

def main():
    # Load your catalog with planetary nebulae
    local_catalog = pd.read_csv("GUVcat_AISxSDSS_HSmaster.csv")
    
    # Convert RA/DEC in the local catalog to SkyCoord objects
    local_coords = SkyCoord(ra=local_catalog['GALEX_RA'].values * u.deg, 
                            dec=local_catalog['GALEX_DEC'].values * u.deg, frame='icrs')

    # Your query template for the S-PLUS data
    query_template = """
    SELECT 
    dual.Field, dual.ID, dual.RA, dual.DEC, 
    dual.X, dual.Y, dual.A, dual.B, dual.ELLIPTICITY, dual.ELONGATION,
    dual.FWHM, dual.KRON_RADIUS, dual.ISOarea, dual.MU_MAX_r, dual.s2n_DET_PStotal,
    dual.SEX_FLAGS_DET, dual.SEX_FLAGS_r,
    
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
    psf.j0861_psf, psf.e_j0861_psf
    FROM "idr5"."idr5_dual" AS dual
    LEFT OUTER JOIN "idr5"."idr5_psf" AS psf ON psf.id = dual.id
    WHERE dual.Field = '{field}'
    """
    
    # Load the S-PLUS field data
    fields = pd.read_csv("iDR5_fields_zps.csv")
    
    # Connect to S-PLUS
    username = input("S-PLUS Username: ")
    password = getpass("S-PLUS Password: ")
    
    try:
        conn = splusdata.connect(username, password)
    except Exception as e:
        print(f"Error connecting to S-PLUS: {e}")
        return
    
    # Create a DataFrame to store the crossmatched results
    crossmatched_table = pd.DataFrame()

    # Iterate over fields in S-PLUS
    for field in fields["Field"]:
        print(f"Querying S-PLUS data for field: {field}")
        query = query_template.format(field=field)
        
        try:
            splus_data = conn.query(query).to_pandas()
        except Exception as e:
            print(f"Error querying field {field}: {e}")
            continue
        
        # Convert S-PLUS RA/DEC to SkyCoord objects
        splus_coords = SkyCoord(ra=splus_data['RA'].values * u.deg, 
                                dec=splus_data['DEC'].values * u.deg, frame='icrs')
        
        # Perform crossmatch using a small separation limit (e.g., 1 arcsec)
        idx, sep2d, _ = splus_coords.match_to_catalog_sky(local_coords)
        sep_constraint = sep2d < 5 * u.arcsec
        matched_splus = splus_data[sep_constraint]
        matched_local = local_catalog.iloc[idx[sep_constraint]]

        # Combine the matched S-PLUS and local data
        matched_table = pd.concat([matched_local.reset_index(drop=True), matched_splus.reset_index(drop=True)], axis=1)

        # Append to the final table
        crossmatched_table = pd.concat([crossmatched_table, matched_table], ignore_index=True)
        
        print(f"Found {len(matched_table)} matches for field {field}")

    # Save the crossmatched results to a CSV file
    crossmatched_table.to_csv("GUVcat_AISxSDSS_HSmaster-splus-5arcsec.csv", index=False)
    print("Crossmatch complete! Results saved to crossmatched_splus_catalog.csv")

if __name__ == "__main__":
    main()
