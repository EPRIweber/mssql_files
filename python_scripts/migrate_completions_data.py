import os
import pyodbc
import re
import sys
from decimal import Decimal, InvalidOperation

# --- Configuration ---
ACCESS_DB_FOLDER = r"C:\Users\pgva005\Documents\EPRI_Prj\Meta data"
MSSQL_SERVER = "DCX-BASQL-D07"
MSSQL_DATABASE = "H2EDGE"
MSSQL_USER = "H2Edge_rw"
try:
    MSSQL_PASSWORD = os.environ['DB_PASS']
except KeyError:
    print("FATAL ERROR: The environment variable 'DB_PASS' is not set.")
    sys.exit(1)

def get_valid_keys(conn_str):
    """
    Connects to SQL Server and fetches sets of all existing unitids and cipcodes
    to be used for validation.
    """
    print("\nConnecting to SQL Server to fetch validation keys...")
    valid_unitids = set()
    valid_cipcodes = set()
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            # Fetch valid unitids
            cursor.execute("SELECT unitid FROM dbo.universities")
            for row in cursor.fetchall():
                valid_unitids.add(row.unitid)
            print(f"-> Found {len(valid_unitids)} unique university IDs.")

            # Fetch valid cipcodes
            cursor.execute("SELECT cipcode FROM dbo.programs")
            for row in cursor.fetchall():
                valid_cipcodes.add(row.cipcode) # Already in decimal format
            print(f"-> Found {len(valid_cipcodes)} unique program (CIP) codes.")

            return valid_unitids, valid_cipcodes
    except pyodbc.Error as e:
        print(f"FATAL ERROR: Could not fetch validation keys from SQL Server: {e}")
        sys.exit(1)

def get_access_tables(access_cursor):
    """Retrieves a list of user-created table names from an Access database."""
    user_tables = []
    for row in access_cursor.tables(tableType='TABLE'):
        if not row.table_name.startswith('MSys') and not row.table_name.startswith('~'):
            user_tables.append(row.table_name)
    return user_tables

def clean_cipcode(cip_str):
    """
    Cleans the '="<value>"' format from cipcode strings and converts to a Decimal object.
    """
    if cip_str is None:
        return None
    try:
        # Clean the string and convert to Decimal
        cleaned_str = cip_str.replace('="', '').replace('"', '')
        # Return the raw decimal for validation purposes
        return Decimal(cleaned_str)
        
    except (InvalidOperation, TypeError):
        return None # Return None if conversion fails

def extract_completions_data_from_access_db(db_path, valid_unitids, valid_cipcodes):
    """
    Connects to an Access database, finds 'C..._A' tables,
    and extracts completions data, filtering by valid keys and data constraints.
    """
    data_for_upsert = []
    # Regex to find tables named like 'C2022_A', 'C2021_A', etc.
    completions_table_pattern = re.compile(r'^C(\d{4})_A$', re.IGNORECASE)
    
    valid_award_levels = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21}

    try:
        access_conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                           f'DBQ={db_path};')
        with pyodbc.connect(access_conn_str) as access_conn:
            access_cursor = access_conn.cursor()
            print(f"\nSuccessfully connected to {os.path.basename(db_path)}")

            all_tables = get_access_tables(access_cursor)
            for table_name in all_tables:
                match = completions_table_pattern.match(table_name)
                if match:
                    report_year = int(match.group(1))
                    print(f"  -> Found matching table: '{table_name}'. Parsing data for year {report_year}...")

                    try:
                        columns_in_table = {row.column_name.upper() for row in access_cursor.columns(table=table_name)}
                        required_columns = {'UNITID', 'CIPCODE', 'AWLEVEL', 'CTOTALT'}

                        if required_columns.issubset(columns_in_table):
                            query = f"SELECT [UNITID], [CIPCODE], [AWLEVEL], [CTOTALT] FROM [{table_name}]"
                            rows = access_cursor.execute(query).fetchall()
                            
                            extracted_count = 0
                            skipped_count = 0

                            for row in rows:
                                unitid = int(row.UNITID) if row.UNITID is not None else None
                                cipcode_decimal = clean_cipcode(row.CIPCODE)
                                award_level = int(row.AWLEVEL) if row.AWLEVEL is not None else None
                                total_completions = int(row.CTOTALT) if row.CTOTALT is not None else None

                                # Validate against the decimal version of the cipcode
                                if (unitid in valid_unitids and
                                    cipcode_decimal in valid_cipcodes and
                                    award_level in valid_award_levels and
                                    total_completions is not None and total_completions >= 0):
                                    
                                    # Format the cipcode as a string with 4 decimal places before sending.
                                    cipcode_string = format(cipcode_decimal.quantize(Decimal('0.0001')))
                                    
                                    data_for_upsert.append((
                                        report_year,
                                        unitid,
                                        cipcode_string,
                                        award_level,
                                        total_completions
                                    ))
                                    extracted_count += 1
                                else:
                                    skipped_count += 1
                            
                            print(f"     ...extracted {extracted_count} valid records.")
                            if skipped_count > 0:
                                print(f"     ...skipped {skipped_count} records due to invalid keys or data values.")
                        else:
                            print(f"     ...SKIPPING table '{table_name}' due to missing required columns.")
                    except pyodbc.Error as ex:
                         print(f"     ...An unexpected error occurred processing table '{table_name}': {ex}")
    except pyodbc.Error as e:
        print(f"Error connecting to or reading from {os.path.basename(db_path)}: {e}")
    return data_for_upsert

def upsert_completions_to_sql_server(data, conn_str):
    """
    Connects to SQL Server and executes the upsert_completions stored procedure.
    """
    if not data:
        print("\nNo valid data to upsert. Exiting.")
        return

    print(f"\nConnecting to SQL Server for final upsert...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as mssql_conn:
            print("Successfully connected to SQL Server.")
            cursor = mssql_conn.cursor()
            
            sql_command = "{CALL dbo.upsert_completions (?)}"
            total_records = len(data)
            chunk_size = 10000 
            
            print(f"Preparing to upsert {total_records} records in chunks of {chunk_size}...")

            for i in range(0, total_records, chunk_size):
                chunk = data[i:i + chunk_size]
                print(f"  -> Upserting records {i+1} to {min(i + chunk_size, total_records)}...")
                cursor.execute(sql_command, [chunk])

            print("\nUpsert operation finished successfully.")
    except pyodbc.Error as e:
        print(f"FATAL ERROR during SQL Server upsert operation: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")

def main():
    """Main function to drive the migration process."""
    print("--- Starting IPEDS Completions Data Migration ---")

    if not os.path.isdir(ACCESS_DB_FOLDER):
        print(f"FATAL ERROR: The specified folder does not exist: {ACCESS_DB_FOLDER}")
        return

    mssql_conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={MSSQL_SERVER};'
        f'DATABASE={MSSQL_DATABASE};'
        f'UID={MSSQL_USER};'
        f'PWD={MSSQL_PASSWORD};'
        'Encrypt=yes;'
        'TrustServerCertificate=yes;'
    )

    valid_unitids, valid_cipcodes = get_valid_keys(mssql_conn_str)
    all_data_to_upsert = []
    
    access_files = [f for f in os.listdir(ACCESS_DB_FOLDER) if f.lower().endswith(('.accdb', '.mdb'))]
    if not access_files:
        print(f"No Access database files found in '{ACCESS_DB_FOLDER}'.")
        return
    print(f"\nFound {len(access_files)} Access files to process.")

    for filename in access_files:
        full_path = os.path.join(ACCESS_DB_FOLDER, filename)
        extracted_data = extract_completions_data_from_access_db(full_path, valid_unitids, valid_cipcodes)
        if extracted_data:
            all_data_to_upsert.extend(extracted_data)
            
    # *** NEW: Aggregate data to prevent MERGE conflicts from duplicate source rows ***
    print("\nAggregating extracted data to handle duplicates...")
    aggregated_data = {}
    for row in all_data_to_upsert:
        report_year, unitid, cipcode, award_level, completions = row
        key = (report_year, unitid, cipcode, award_level)
        
        # Sum completions for duplicate keys
        aggregated_data[key] = aggregated_data.get(key, 0) + completions

    # Convert the aggregated dictionary back into the list of tuples format
    final_data_for_upsert = [key + (total_completions,) for key, total_completions in aggregated_data.items()]
    print(f"-> Aggregation complete. Original records: {len(all_data_to_upsert)}, Final unique records: {len(final_data_for_upsert)}")


    # Pass the clean, aggregated data to the upsert function
    upsert_completions_to_sql_server(final_data_for_upsert, mssql_conn_str)

    print("\n--- Migration Process Finished ---")

if __name__ == "__main__":
    main()
