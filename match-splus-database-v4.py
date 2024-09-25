import pandas as pd
import splusdata
from getpass import getpass

def main():
    # Cargar tu catálogo con las coordenadas de nebulosas planetarias
    local_catalog = pd.read_csv("GUVcat_AISxSDSS_HSmaster.csv")
    
    # Cargar los campos de S-PLUS
    fields = pd.read_csv("iDR5_fields_zps.csv")
    
    # Conectar a S-PLUS
    username = input("S-PLUS Username: ")
    password = getpass("S-PLUS Password: ")
    
    try:
        conn = splusdata.connect(username, password)
    except Exception as e:
        print(f"Error al conectar a S-PLUS: {e}")
        return

    # Crear un DataFrame para almacenar los resultados del crossmatch
    crossmatched_table = pd.DataFrame()

    # Iterar sobre los registros del catálogo local
    for index, row in local_catalog.iterrows():
        local_ra = row['GALEX_RA']
        local_dec = row['GALEX_DEC']

        print(f"Consultando objetos cercanos a RA: {local_ra}, DEC: {local_dec}")
        
        # Plantilla de consulta alternativa para el crossmatch
        query_template = f"""
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
            psf.i_psf, psf.e_i_psf
        FROM "idr5"."idr5_dual" AS dual
        LEFT OUTER JOIN "idr5"."idr5_psf" AS psf ON psf.id = dual.id
        WHERE dual.Field IN ({','.join(f"'{field}'" for field in fields["Field"].astype(str))})
        AND 60 * 60 * 2.0 * DEGREES(ACOS(SIN(RADIANS({local_dec})) * SIN(RADIANS(dual.DEC)) + COS(RADIANS({local_dec})) * COS(RADIANS(dual.DEC)) * COS(RADIANS(dual.RA - {local_ra})))) <= 1.0
        """
        
        # Imprimir la consulta para depuración
        print("Consulta ADQL:", query_template)

        # Ejecutar la consulta y obtener los resultados
        try:
            splus_data = conn.query(query_template).to_pandas()
        except Exception as e:
            print(f"Error al consultar cerca de RA: {local_ra}, DEC: {local_dec}: {e}")
            continue

        # Agregar los datos coincidentes a la tabla final
        splus_data['GALEX_RA'] = local_ra
        splus_data['GALEX_DEC'] = local_dec
        crossmatched_table = pd.concat([crossmatched_table, splus_data], ignore_index=True)

    # Guardar los resultados del crossmatch en un archivo CSV
    crossmatched_table.to_csv("GUVcat_AISxSDSS_HSmaster_splus_crossmatched.csv", index=False)
    print("¡Crossmatch completo! Resultados guardados en GUVcat_AISxSDSS_HSmaster_splus_crossmatched.csv")

if __name__ == "__main__":
    main()
