import os
import pyodbc
import re
import sys

# --- Configuration ---
# IMPORTANT: Set the path to the folder containing your Access DB files.
ACCESS_DB_FOLDER = r"C:\Users\pgva005\Documents\EPRI_Prj\Meta data"

# IMPORTANT: Set your SQL Server connection details.
# The password is read from an environment variable for security.
MSSQL_SERVER = "DCX-BASQL-D07"
MSSQL_DATABASE = "H2EDGE"
MSSQL_USER = "H2Edge_rw"
try:
    # Attempt to get the password from an environment variable named DB_PASS.
    # In Windows Command Prompt: set DB_PASS=your_password
    # In PowerShell: $env:DB_PASS="your_password"
    MSSQL_PASSWORD = os.environ['DB_PASS']
except KeyError:
    print("FATAL ERROR: The environment variable 'DB_PASS' is not set.")
    print("Please set it to your database password before running the script.")
    sys.exit(1) # Exit the script if the password isn't found.

def get_valid_unitids(conn_str):
    """
    Connects to SQL Server and fetches a set of all existing unitids
    from the universities table to be used for validation.
    """
    print("\nConnecting to SQL Server to fetch valid university IDs...")
    valid_ids = set()
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT unitid FROM dbo.universities")
            rows = cursor.fetchall()
            for row in rows:
                valid_ids.add(row.unitid)
            print(f"-> Found {len(valid_ids)} unique university IDs in the destination table.")
            return valid_ids
    except pyodbc.Error as e:
        print(f"FATAL ERROR: Could not fetch unitids from SQL Server: {e}")
        sys.exit(1) # Exit if we can't get the validation list.

def get_access_tables(access_cursor):
    """
    Retrieves a list of user-created table names from an Access database.
    This method uses the standard odbc cursor.tables() function to avoid
    permission errors on the MSysObjects system table.
    """
    user_tables = []
    # Fetch all tables of type 'TABLE'
    for row in access_cursor.tables(tableType='TABLE'):
        table_name = row.table_name
        # Filter out system and temporary tables, which often start with 'MSys' or '~'
        if not table_name.startswith('MSys') and not table_name.startswith('~'):
            user_tables.append(table_name)
    return user_tables

def extract_data_from_access_db(db_path, valid_unitids):
    """
    Connects to a single Access database, finds relevant 'GR' tables,
    and extracts graduation rate data, filtering by valid unitids.

    Args:
        db_path (str): The full path to the .accdb or .mdb file.
        valid_unitids (set): A set of unitids that exist in the destination table.

    Returns:
        list: A list of tuples, where each tuple contains data for one record
              in the format (report_year, unitid, grtype, grrtot).
    """
    data_for_upsert = []
    # Regex to find tables named like 'GR2022', 'GR2021', etc.
    gr_table_pattern = re.compile(r'^GR(\d{4})$', re.IGNORECASE)

    try:
        access_conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                           f'DBQ={db_path};')
        with pyodbc.connect(access_conn_str) as access_conn:
            access_cursor = access_conn.cursor()
            print(f"\nSuccessfully connected to {os.path.basename(db_path)}")

            all_tables = get_access_tables(access_cursor)
            for table_name in all_tables:
                match = gr_table_pattern.match(table_name)
                if match:
                    report_year = int(match.group(1))
                    print(f"  -> Found matching table: '{table_name}'. Parsing data for year {report_year}...")

                    try:
                        columns_in_table = {row.column_name.upper() for row in access_cursor.columns(table=table_name)}
                        required_columns = {'UNITID', 'GRTYPE', 'GRTOTLT'}

                        if required_columns.issubset(columns_in_table):
                            query = f"SELECT [UNITID], [GRTYPE], [GRTOTLT] FROM [{table_name}]"
                            rows = access_cursor.execute(query).fetchall()
                            
                            extracted_count = 0
                            skipped_count = 0

                            for row in rows:
                                # *** FIX: Validate unitid before appending data ***
                                if row.UNITID is not None and int(row.UNITID) in valid_unitids:
                                    if row.GRTYPE is not None and row.GRTOTLT is not None:
                                        data_for_upsert.append((
                                            report_year,
                                            int(row.UNITID),
                                            int(row.GRTYPE),
                                            int(row.GRTOTLT)
                                        ))
                                        extracted_count += 1
                                else:
                                    skipped_count += 1
                            
                            print(f"     ...extracted {extracted_count} valid records.")
                            if skipped_count > 0:
                                print(f"     ...skipped {skipped_count} records due to missing or invalid unitid.")
                        else:
                            print(f"     ...SKIPPING table '{table_name}' because it does not contain the required columns (UNITID, GRTYPE, GRTOTLT).")
                    except pyodbc.Error as ex:
                         print(f"     ...An unexpected error occurred processing table '{table_name}': {ex}")
                elif table_name.upper().startswith('GR'):
                    print(f"  -> Ignoring non-standard table: '{table_name}'")
    except pyodbc.Error as e:
        print(f"Error connecting to or reading from {os.path.basename(db_path)}: {e}")
    return data_for_upsert

def upsert_to_sql_server(data, conn_str):
    """
    Connects to SQL Server and executes the upsert stored procedure
    using a table-valued parameter, processing the data in chunks.
    """
    if not data:
        print("\nNo valid data to upsert. Exiting.")
        return

    print(f"\nConnecting to SQL Server for final upsert...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as mssql_conn:
            print("Successfully connected to SQL Server.")
            cursor = mssql_conn.cursor()
            
            sql_command = "{CALL dbo.upsert_graduation_rates (?)}"
            total_records = len(data)
            chunk_size = 10000 
            
            print(f"Preparing to upsert {total_records} records in chunks of {chunk_size}...")

            for i in range(0, total_records, chunk_size):
                chunk = data[i:i + chunk_size]
                print(f"  -> Upserting records {i+1} to {min(i + chunk_size, total_records)}...")
                # By pre-validating the data, we prevent the errors that corrupted the cursor state.
                cursor.execute(sql_command, [chunk])

            print("\nUpsert operation finished successfully.")
    except pyodbc.Error as e:
        # Any error here is now unexpected and should be treated as fatal.
        print(f"FATAL ERROR during SQL Server upsert operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

def main():
    """Main function to drive the migration process."""
    print("--- Starting IPEDS Graduation Rate Data Migration ---")

    if not os.path.isdir(ACCESS_DB_FOLDER):
        print(f"FATAL ERROR: The specified folder does not exist: {ACCESS_DB_FOLDER}")
        return

    # Define the connection string once
    mssql_conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={MSSQL_SERVER};'
        f'DATABASE={MSSQL_DATABASE};'
        f'UID={MSSQL_USER};'
        f'PWD={MSSQL_PASSWORD};'
        'Encrypt=yes;'
        'TrustServerCertificate=yes;'
    )

    # 1. Get the list of valid UnitIDs from the destination table first.
    valid_unitids = get_valid_unitids(mssql_conn_str)

    all_data_to_upsert = []
    
    access_files = [f for f in os.listdir(ACCESS_DB_FOLDER) if f.lower().endswith(('.accdb', '.mdb'))]
    if not access_files:
        print(f"No Access database files (.accdb, .mdb) found in '{ACCESS_DB_FOLDER}'.")
        return
    print(f"\nFound {len(access_files)} Access files to process.")

    # 2. Extract data, using the valid_unitids set to filter records.
    for filename in access_files:
        full_path = os.path.join(ACCESS_DB_FOLDER, filename)
        extracted_data = extract_data_from_access_db(full_path, valid_unitids)
        if extracted_data:
            all_data_to_upsert.extend(extracted_data)

    # 3. Perform the bulk upsert with the clean, validated data.
    upsert_to_sql_server(all_data_to_upsert, mssql_conn_str)

    print("\n--- Migration Process Finished ---")

if __name__ == "__main__":
    main()
