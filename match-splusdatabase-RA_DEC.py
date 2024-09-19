import splusdata
import getpass
import pandas as pd
from astropy.table import Table, vstack
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Connecting with S-PLUS database
username = str(input("Login: "))
password = getpass.getpass("Password: ")

try:
    conn = splusdata.connect(username, password)
except Exception as e:
    print(f"Error connecting to S-PLUS database: {e}")
    exit()

# Loading the input CSV file
file_name = "GUVcat_AISxSDSS_HSmaster.csv"
df = pd.read_csv(file_name)
print("Number of objects:", len(df))

# Define the optimized query
Query = """
SELECT dual.Field, dual.ID, dual.RA, dual.DEC, 
       dual.FWHM, dual.KRON_RADIUS, 
       dual.r_PStotal, dual.e_r_PStotal, 
       dual.J0660_PStotal, dual.e_J0660_PStotal, 
       dual.i_PStotal, dual.e_i_PStotal
FROM TAP_UPLOAD.upload AS tap
LEFT OUTER JOIN idr5.idr5_dual AS dual 
    ON (1=CONTAINS( POINT('ICRS', dual.RA, dual.DEC), 
        CIRCLE('ICRS', tap.GALEX_RA, tap.GALEX_DEC, 0.000555555555556)))
"""

# Reduce the chunk size for smaller requests
chunk_size = 20  # Try a smaller value to reduce the request size
n = (len(df) + chunk_size - 1) // chunk_size
print('Number of chunks:', n)

df_chunks = [df.iloc[i*chunk_size:(i+1)*chunk_size] for i in range(n)]

# Define function to query each chunk
def query_chunk(a, chunk):
    start_time = datetime.now()
    print(f"Processing chunk {a}... (Start: {start_time})")
    try:
        results = conn.query(Query, chunk)
        if isinstance(results, Table):
            print(f"Processing chunk {a} as Table...")
            return results
        elif isinstance(results, pd.DataFrame):
            print(f"Processing chunk {a} as DataFrame...")
            return Table.from_pandas(results)
        else:
            print(f"Unexpected results type for chunk {a}: {type(results)}")
            return None
    except Exception as e:
        print(f"Error occurred while querying chunk {a}: {e}")
        return None
    finally:
        end_time = datetime.now()
        print(f"Chunk {a} processed (End: {end_time}, Duration: {end_time - start_time})")

# Use ThreadPoolExecutor to parallelize the queries
merged_table_list = []
with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust number of workers if necessary
    futures = [executor.submit(query_chunk, i, chunk) for i, chunk in enumerate(df_chunks)]
    for future in futures:
        result = future.result()
        if result:
            merged_table_list.append(result)
        time.sleep(2)  # Optional: Increase delay to prevent hitting server rate limits

# Merge all results if any
if merged_table_list:
    merged_table = vstack(merged_table_list)  # Combine the results
    print("Number of objects with match:", len(merged_table))

    # Save the result
    output_file = Path(file_name).stem + "-splus-normal.csv"
    merged_table.to_pandas().to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")
else:
    print("No matching objects found.")
