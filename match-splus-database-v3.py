import splusdata
import getpass
import pandas as pd
from astropy.table import Table, vstack
from pathlib import Path
  
ROOT = Path("Final-list/") 
  
# Connecting with SPLUS database
username = str(input("Login: "))
password = getpass.getpass("Password: ")

conn = splusdata.connect(username, password)
  
# Loading the input CSV file
file_name = "GUVcat_AISxSDSS_HSmaster.csv"
df = pd.read_csv(file_name)
print("Number of objects:", len(df))

# Split the dataframe into smaller chunks
chunk_size = 100  # Reduced chunk size to fit within server's size limit
n = (len(df) + chunk_size - 1) // chunk_size  # Number of chunks
df_chunks = [df.iloc[i*chunk_size:(i+1)*chunk_size] for i in range(n)]

# Define the query template
query_template = """
    SELECT detection.Field, detection.ID, detection.RA, detection.DEC,
           detection.FWHM, detection.ISOarea, detection.KRON_RADIUS, 
           detection.CLASS_STAR, detection.u_PStotal, detection.J0378_PStotal, 
           detection.J0395_PStotal, detection.J0410_PStotal, detection.J0430_PStotal, 
           detection.g_PStotal, detection.J0515_PStotal, detection.r_PStotal, 
           detection.J0660_PStotal, detection.i_PStotal, 
           detection.J0861_PStotal, detection.z_PStotal
    FROM TAP_UPLOAD.upload AS tap
    LEFT OUTER JOIN idr5.idr5_dual AS detection
    ON (1=CONTAINS( POINT('ICRS', detection.RA, detection.DEC), 
        CIRCLE('ICRS', tap.GALEX_RA, tap.GALEX_DEC, 0.000555555555556)))
"""

# Initialize an empty list to store query results
merged_table_list = []

# Loop through chunks and query each chunk
for a in range(n):
    print(f"Processing chunk {a+1}/{n}...")
    chunk_df = df_chunks[a]
    try:
        # Perform the query
        results = conn.query(query_template, chunk_df)
        if results:
            merged_table_list.append(results)
    except Exception as e:
        print(f"Error occurred while processing chunk {a+1}: {e}")
  
# Merge all result astropy tables if any
if merged_table_list:
    merged_table = vstack(merged_table_list)
    print("Number of objects with match:", len(merged_table))
  
    # Save the result to CSV
    asciifile = "GUVcat_AISxSDSS_HSmaster-normal.csv"
    merged_table.to_pandas().to_csv(asciifile, index=False)
    print(f"Results saved to {asciifile}")
else:
    print("No matching objects found.")
