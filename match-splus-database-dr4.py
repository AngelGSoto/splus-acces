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
            det.Field, det.ID, det.RA, det.DEC, det.X, det.Y,
            det.FWHM,  det.FWHM_n, det.ISOarea, det.KRON_RADIUS, 
            det.MU_MAX_INST, det.PETRO_RADIUS, det.SEX_FLAGS_DET, det.SEX_NUMBER_DET, det.CLASS_STAR,
            det.s2n_DET_PStotal, det.THETA, det.ELLIPTICITY, det.ELONGATION,
            det.FLUX_RADIUS_20, det.FLUX_RADIUS_50, det.FLUX_RADIUS_70, det.FLUX_RADIUS_90,  
            r.s2n_r_PStotal, j0660.s2n_J0660_PStotal, i.s2n_i_PStotal,
            r.FWHM_r, r.FWHM_n_r, j0660.FWHM_J0660, j0660.FWHM_n_J0660, i.FWHM_i, i.FWHM_n_i,
            r.SEX_FLAGS_r, j0660.SEX_FLAGS_J0660, i.SEX_FLAGS_i,
            rs.CLASS_STAR_r, j0660s.CLASS_STAR_J0660, isf.CLASS_STAR_i,
        
            r.r_PStotal, r.e_r_PStotal,
            g.g_PStotal, g.e_g_PStotal,
            i.i_PStotal, i.e_i_PStotal,
            u.u_PStotal, u.e_u_PStotal,
            z.z_PStotal, z.e_z_PStotal,
            j0378.j0378_PStotal, j0378.e_j0378_PStotal,
            j0395.j0395_PStotal, j0395.e_j0395_PStotal,
            j0410.j0410_PStotal, j0410.e_j0410_PStotal,
            j0430.j0430_PStotal, j0430.e_j0430_PStotal,
            j0515.j0515_PStotal, j0515.e_j0515_PStotal,
            j0660.j0660_PStotal, j0660.e_j0660_PStotal,
            j0861.j0861_PStotal, j0861.e_j0861_PStotal,
        
            rs.r_psf, rs.e_r_psf,
            gs.g_psf, gs.e_g_psf,
            isf.i_psf, isf.e_i_psf, -- isf (is -> reserved)
            us.u_psf, us.e_u_psf,
            zs.z_psf, zs.e_z_psf,
            j0378s.j0378_psf, j0378s.e_j0378_psf,
            j0395s.j0395_psf, j0395s.e_j0395_psf,
            j0410s.j0410_psf, j0410s.e_j0410_psf,
            j0430s.j0430_psf, j0430s.e_j0430_psf,
            j0515s.j0515_psf, j0515s.e_j0515_psf,
            j0660s.j0660_psf, j0660s.e_j0660_psf,
            j0861s.j0861_psf, j0861s.e_j0861_psf
        
        FROM "idr4_dual"."idr4_detection_image" AS det 
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_r" AS r ON r.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_g" AS g ON g.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_i" AS i ON i.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_z" AS z ON z.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_u" AS u ON u.id = det.id
        
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0378" AS j0378 ON j0378.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0395" AS j0395 ON j0395.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0410" AS j0410 ON j0410.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0430" AS j0430 ON j0430.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0515" AS j0515 ON j0515.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0660" AS j0660 ON j0660.id = det.id
        LEFT OUTER JOIN "idr4_dual"."idr4_dual_j0861" AS j0861 ON j0861.id = det.id
        
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_r" AS rs ON rs.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_g" AS gs ON gs.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_i" AS isf ON isf.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_z" AS zs ON zs.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_u" AS us ON us.id = det.id
        
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0378" AS j0378s ON j0378s.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0395" AS j0395s ON j0395s.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0410" AS j0410s ON j0410s.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0430" AS j0430s ON j0430s.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0515" AS j0515s ON j0515s.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0660" AS j0660s ON j0660s.id = det.id
        LEFT OUTER JOIN "idr4_psf"."idr4_psf_j0861" AS j0861s ON j0861s.id = det.id
       WHERE det.Field = '{field}'
    """
    
    # Load the S-PLUS field data
    fields = pd.read_csv("https://splus.cloud/files/documentation/iDR4/tabelas/iDR4_zero-points.csv")
    
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
        sep_constraint = sep2d < 2 * u.arcsec
        matched_splus = splus_data[sep_constraint]
        matched_local = local_catalog.iloc[idx[sep_constraint]]

        # Combine the matched S-PLUS and local data
        matched_table = pd.concat([matched_local.reset_index(drop=True), matched_splus.reset_index(drop=True)], axis=1)

        # Append to the final table
        crossmatched_table = pd.concat([crossmatched_table, matched_table], ignore_index=True)
        
        print(f"Found {len(matched_table)} matches for field {field}")

    # Save the crossmatched results to a CSV file
    crossmatched_table.to_csv("GUVcat_AISxSDSS_HSmaster_splus_dr4.csv", index=False)
    print("Crossmatch complete! Results saved to crossmatched_splus_catalog.csv")

if __name__ == "__main__":
    main()
