import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
import glob
import csv
import re
import chardet
from sqlalchemy import create_engine
import numpy as np

def create_connection(host_name, user_name, user_password, db_name):
    """Create a connection to MySQL database"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            charset='utf8mb4',  # This is correct for MySQL
            use_unicode=True
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def get_csv_files(folder_path):
    """Get all CSV files in the specified folder"""
    return glob.glob(os.path.join(folder_path, "**", "*.csv"), recursive=True)

def detect_file_encoding(file_path):
    """Detect the encoding of a file"""
    try:
        with open(file_path, 'rb') as f:
            sample = f.read(10000)  # Read first 10KB
            result = chardet.detect(sample)
            encoding = result['encoding'] if result['encoding'] else 'utf-8'
            # Make sure we never return utf8mb4 as it's not a valid Python encoding
            if encoding.lower() == 'utf8mb4':
                encoding = 'utf-8'
            return encoding
    except Exception as e:
        print(f"Error detecting encoding: {e}")
        return 'utf-8'  # Default to utf-8

def sanitize_table_name(name):
    """Sanitize table name to be MySQL compatible"""
    # Replace non-alphanumeric characters with underscore
    sanitized = re.sub(r'[^\w]', '_', name)
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'tbl_' + sanitized
    return sanitized

def sanitize_column_name(name):
    """Sanitize column name to be MySQL compatible"""
    # Handle empty or None column names
    if name is None or name == "":
        return "unnamed_column"
    
    # Replace non-alphanumeric characters with underscore
    sanitized = re.sub(r'[^\w]', '_', str(name))
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = 'col_' + sanitized
    
    # MySQL reserved words check
    reserved_words = ["add", "all", "alter", "analyze", "and", "as", "asc", "between", 
                     "by", "case", "check", "column", "constraint", "create", "database", 
                     "delete", "desc", "distinct", "drop", "else", "end", "exists", 
                     "foreign", "from", "group", "having", "in", "index", "insert", 
                     "into", "is", "join", "key", "like", "limit", "not", "null", 
                     "on", "or", "order", "primary", "procedure", "references", "select", 
                     "set", "table", "then", "to", "trigger", "union", "unique", "update", 
                     "using", "values", "when", "where"]
    
    if sanitized.lower() in reserved_words:
        sanitized = sanitized + '_col'
    
    return sanitized

def infer_schema_from_csv(csv_file):
    """Infer schema (column names and types) from CSV file"""
    try:
        # Detect file encoding
        encoding = detect_file_encoding(csv_file)
        print(f"Detected encoding: {encoding}")
        
        # Count total rows for progress reporting
        try:
            with open(csv_file, 'r', encoding=encoding, errors='replace') as f:
                total_rows = sum(1 for _ in f) - 1  # Subtract header
        except Exception as e:
            print(f"Error counting rows: {e}")
            total_rows = None
            
        # Process in chunks to handle large files
        chunk_size = 10000
        column_types = {}
        
        with tqdm(total=total_rows, desc="Inferring schema") as pbar:
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size, encoding=encoding, 
                                     on_bad_lines='skip', low_memory=False):
                
                # Sanitize column names
                sanitized_columns = [sanitize_column_name(col) for col in chunk.columns]
                chunk.columns = sanitized_columns
                
                # Analyze each column
                for col in chunk.columns:
                    if pd.api.types.is_integer_dtype(chunk[col]):
                        mysql_type = "INT"
                    elif pd.api.types.is_float_dtype(chunk[col]):
                        mysql_type = "DOUBLE"
                    elif pd.api.types.is_datetime64_dtype(chunk[col]):
                        mysql_type = "DATETIME"
                    elif pd.api.types.is_bool_dtype(chunk[col]):
                        mysql_type = "BOOLEAN"
                    else:
                        # String or object type - check length
                        try:
                            max_len = chunk[col].astype(str).str.len().max()
                            if max_len < 255:
                                mysql_type = f"VARCHAR({max_len + 50})"  # Add padding
                            else:
                                mysql_type = "TEXT"
                        except:
                            mysql_type = "TEXT"
                    
                    # Update column type if needed
                    if col not in column_types:
                        column_types[col] = mysql_type
                    elif "TEXT" in mysql_type and "VARCHAR" in column_types[col]:
                        # Upgrade to TEXT if needed
                        column_types[col] = mysql_type
                
                # Update progress
                if pbar.total:
                    pbar.update(len(chunk))
        
        # Create final schema
        columns = []
        for col, dtype in column_types.items():
            safe_col_name = f"`{col}`"
            columns.append((safe_col_name, dtype))
        
        return columns
    
    except Exception as e:
        print(f"Error inferring schema: {e}")
        # Fallback: use basic schema
        try:
            with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f)
                header = next(reader)
                columns = []
                
                for col in header:
                    safe_col_name = f"`{sanitize_column_name(col)}`"
                    columns.append((safe_col_name, "VARCHAR(255)"))
                
                return columns
        except Exception as e2:
            print(f"Fallback schema inference failed: {e2}")
            return []

def create_table(connection, table_name, columns):
    """Create a table with the specified columns"""
    cursor = connection.cursor()
    try:
        # Drop table if it exists
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        
        # Create table
        create_table_query = f"CREATE TABLE `{table_name}` ("
        create_table_query += ", ".join([f"{col[0]} {col[1]}" for col in columns])
        create_table_query += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' created successfully")
        return True
    except Error as e:
        print(f"The error '{e}' occurred")
        return False
    finally:
        cursor.close()

def import_csv_to_table(connection, csv_file, table_name):
    """Import data from CSV file to MySQL table"""
    try:
        # Detect file encoding
        encoding = detect_file_encoding(csv_file)
        
        # Get total rows for progress bar
        try:
            with open(csv_file, 'r', encoding=encoding, errors='replace') as f:
                total_rows = sum(1 for _ in f) - 1  # Subtract header
        except Exception as e:
            print(f"Error counting rows: {e}")
            total_rows = None
        
        # Create SQLAlchemy engine for efficient import
        user = connection.user
        password = connection._password
        host = connection._host
        database = connection._database
        
        engine_url = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
        engine = create_engine(engine_url)
        
        # Read and import in chunks with progress bar
        chunk_size = 10000
        with tqdm(total=total_rows, desc=f"Importing {table_name}") as pbar:
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size, encoding=encoding, 
                                     on_bad_lines='skip', low_memory=False):
                # Sanitize column names
                chunk.columns = [sanitize_column_name(col) for col in chunk.columns]
                
                # Replace inf/-inf with NaN (MySQL doesn't support infinity)
                for col in chunk.select_dtypes(include=['float', 'float64']).columns:
                    chunk[col] = chunk[col].replace([np.inf, -np.inf], np.nan)
                
                # Insert data
                chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                
                # Update progress bar
                if pbar.total:
                    pbar.update(len(chunk))
        
        print(f"Data imported successfully into table '{table_name}'")
        return True
    except Exception as e:
        print(f"SQLAlchemy import failed: {e}")
        print("Trying alternative import method...")
        
        try:
            # Fallback to manual insert
            cursor = connection.cursor()
            
            # Read CSV manually
            df = pd.read_csv(csv_file, encoding=encoding, on_bad_lines='skip')
            df.columns = [sanitize_column_name(col) for col in df.columns]
            
            # Replace inf/-inf with NULL for MySQL
            for col in df.select_dtypes(include=['float', 'float64']).columns:
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            
            # Prepare SQL query for inserting data
            placeholders = ", ".join(["%s"] * len(df.columns))
            columns = ", ".join([f"`{col}`" for col in df.columns])
            insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples for insertion
            values = [tuple(x) for x in df.replace({np.nan: None}).values]
            
            # Insert data in batches
            batch_size = 1000
            for i in tqdm(range(0, len(values), batch_size), desc=f"Importing {table_name}"):
                batch = values[i:i+batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()
            
            cursor.close()
            print(f"Data imported using manual method into table '{table_name}'")
            return True
        except Error as e2:
            print(f"Alternative import failed: {e2}")
            return False

def main():
    # Database connection parameters
    host = "localhost"
    user = "ali"
    password = "admin"
    database = "UNOX"
    dataset_folder = "dataset"
    
    # Create connection to MySQL
    connection = create_connection(host, user, password, database)
    
    if connection is not None:
        try:
            # Get all CSV files in the dataset folder
            csv_files = get_csv_files(dataset_folder)
            
            if not csv_files:
                print(f"No CSV files found in '{dataset_folder}'")
                return
            
            print(f"Found {len(csv_files)} CSV files")
            
            # Process each CSV file
            for csv_file in tqdm(csv_files, desc="Processing CSV files"):
                # Generate table name from file name
                base_name = os.path.splitext(os.path.basename(csv_file))[0]
                table_name = sanitize_table_name(base_name)
                
                print(f"\nProcessing {csv_file} -> {table_name}")
                
                # Infer schema from CSV
                columns = infer_schema_from_csv(csv_file)
                
                if not columns:
                    print(f"Failed to infer schema for {csv_file}, skipping")
                    continue
                
                # Create table
                if create_table(connection, table_name, columns):
                    # Import data
                    import_csv_to_table(connection, csv_file, table_name)
            
            print("\nAll CSV files processed successfully")
        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        
        finally:
            connection.close()

if __name__ == "__main__":
    main()