import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import glob
from tqdm import tqdm
import re
import numpy as np
import time
import sys
import argparse

def create_connection(host_name, user_name, user_password, db_name):
    """Create a connection to MySQL database"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            charset='utf8mb4',  
            use_unicode=True
        )
        print(f"MySQL/{db_name} connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def get_csv_files(folder_path):
    """Get all CSV files in the specified folder"""
    csv_files = glob.glob(os.path.join(folder_path, "**", "*.csv"), recursive=True)
    if not csv_files:
        print(f"No CSV files found in '{folder_path}' or its subdirectories")
        # Check if the folder exists
        if not os.path.exists(folder_path):
            print(f"The folder '{folder_path}' does not exist!")
        else:
            # List contents of the folder to debug
            print(f"Contents of '{folder_path}' folder:")
            for item in os.listdir(folder_path):
                print(f"  - {item}")
    return csv_files

def map_csv_to_table(filename):
    """Map CSV filename to database table name based on conventions"""
    base_name = os.path.basename(filename).lower().replace('.csv', '')
    
    # Mapping logic based on filename patterns
    mapping = {
        'screen': 'Screen',
        'seat': 'Seat',
        'movie': 'Movie',
        'moviecast': 'MovieCast',
        'movie_cast': 'MovieCast',
        'review': 'Review',
        'show': 'Show',
        'showseat': 'ShowSeat',
        'show_seat': 'ShowSeat',
        'user': 'User',
        'membership': 'Membership',
        'booking': 'Booking',
        'ticket': 'Ticket',
        'paymentgateway': 'PaymentGateway',
        'payment_gateway': 'PaymentGateway',
        'payment': 'Payment',
        'fooditem': 'FoodItem',
        'food_item': 'FoodItem',
        'fooditemsize': 'FoodItemSize',
        'food_item_size': 'FoodItemSize',
        'foodorder': 'FoodOrder',
        'food_order': 'FoodOrder',
        'foodorderitem': 'FoodOrderItem',
        'food_order_item': 'FoodOrderItem',
        'pointstransaction': 'PointsTransaction',
        'points_transaction': 'PointsTransaction'
    }
    
    # Try direct mapping
    if base_name in mapping:
        return mapping[base_name]
    
    # Try partial matching
    for key in mapping:
        if key in base_name:
            return mapping[key]
    
    # Default to filename with first letter capitalized
    return base_name.capitalize()

def count_table_rows(connection, table_name):
    """Count rows in a table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Error as e:
        print(f"Error counting rows in {table_name}: {e}")
        return 0

def import_data(connection, csv_file, table_name):
    """Import data from CSV file to specified table"""
    try:
        print(f"\nProcessing {os.path.basename(csv_file)} -> {table_name}")
        
        # Get initial row count
        initial_row_count = count_table_rows(connection, table_name)
        print(f"Current row count in {table_name}: {initial_row_count}")
        
        # Create SQLAlchemy engine for efficient import
        user = connection.user
        password = connection._password
        host = connection._host
        database = connection._database
        
        engine_url = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
        engine = create_engine(engine_url)
        
        # Try multiple encodings with progress bar
        encodings = ['utf-8', 'latin1', 'cp1252']
        df = None
        
        with tqdm(total=len(encodings), desc="Trying encodings") as pbar:
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_file, encoding=encoding, on_bad_lines='skip', low_memory=False)
                    print(f"✅ Successfully loaded with {encoding} encoding")
                    pbar.update(1)
                    break
                except UnicodeDecodeError:
                    print(f"❌ Failed with {encoding} encoding, trying another...")
                    pbar.update(1)
                except Exception as e:
                    print(f"❌ Error: {e}")
                    pbar.update(1)
                    continue
        
        if df is None:
            print(f"Could not load {csv_file} with any encoding")
            return False
            
        print(f"CSV file contains {len(df)} rows")
        
        # Show first few rows to debug
        print("First 3 rows of CSV data:")
        print(df.head(3))
        
        # Clean data before importing
        print("Cleaning data...")
        
        # Handle infinities
        float_cols = df.select_dtypes(include=['float', 'float64']).columns
        if not float_cols.empty:
            with tqdm(total=len(float_cols), desc="Processing float columns") as pbar:
                for col in float_cols:
                    df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                    pbar.update(1)
        
        # Convert column names if needed
        # This maps DataFrame columns to table columns based on case insensitive matching
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE `{table_name}`")
        db_columns = [column[0] for column in cursor.fetchall()]
        cursor.close()
        
        print(f"Database columns: {db_columns}")
        print(f"CSV columns: {list(df.columns)}")
        
        column_mapping = {}
        for df_col in df.columns:
            for db_col in db_columns:
                if df_col.lower() == db_col.lower():
                    column_mapping[df_col] = db_col
                    break
        
        if column_mapping:
            print(f"Mapping columns: {column_mapping}")
            df = df.rename(columns=column_mapping)
        
        # Drop columns that don't exist in the table
        df_columns = set(df.columns)
        db_columns_set = set(db_columns)
        columns_to_drop = df_columns - db_columns_set
        
        if columns_to_drop:
            print(f"Dropping columns not in table schema: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)
        
        # Add missing columns with NULL values
        missing_columns = db_columns_set - df_columns
        if missing_columns:
            print(f"Adding missing columns: {missing_columns}")
            for col in missing_columns:
                df[col] = None
        
        # Ensure proper column order to match table schema
        df = df[db_columns]
        
        # Import data to table with progress bar
        print(f"Importing {len(df)} records to {table_name}...")
        
        # Use tqdm to track SQLAlchemy's chunksize-based imports
        chunk_size = 1000
        total_chunks = (len(df) // chunk_size) + (1 if len(df) % chunk_size > 0 else 0)
        
        with tqdm(total=total_chunks, desc=f"Uploading chunks") as pbar:
            # Define a callback for SQLAlchemy to execute after each chunk
            def track_progress(batch):
                pbar.update(1)
                return batch
                
            # Import data
            df.to_sql(
                name=table_name, 
                con=engine, 
                if_exists='append', 
                index=False, 
                chunksize=chunk_size,
                method=track_progress if total_chunks > 1 else None
            )
        
        # Verify data was actually imported
        final_row_count = count_table_rows(connection, table_name)
        rows_added = final_row_count - initial_row_count
        
        if rows_added > 0:
            print(f"✅ Successfully imported {rows_added} rows to {table_name}")
            return True
        else:
            print(f"❌ WARNING: No rows were added to {table_name}! Initial: {initial_row_count}, Final: {final_row_count}")
            return False
        
    except Exception as e:
        print(f"❌ Error importing data to {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Import CSV data into SRM_STEP database')
    parser.add_argument('--dataset', '-d', help='Path to dataset folder', default='dataset')
    args = parser.parse_args()
    
    # Database connection parameters
    host = "localhost"
    user = "ali"
    password = "admin"
    db_name = "SRM_STEP"
    data_folder = args.dataset  # Use the folder specified via command line
    
    print(f"Starting import process from '{data_folder}' to database '{db_name}'")
    print(f"Current working directory: {os.getcwd()}")
    
    # Create connection
    connection = create_connection(host, user, password, db_name)
    if connection is None:
        return
    
    try:
        # List of potential data folder locations to try
        potential_folders = [
            data_folder,  # Original specified path
            os.path.join(os.getcwd(), data_folder),  # Path relative to current dir
            os.path.join(os.getcwd(), '..', data_folder),  # Parent directory
            os.path.join(os.getcwd(), '..', 'V1', data_folder),  # V1 directory
            os.path.join(os.getcwd(), '..', 'dataset'),  # Parent dir's dataset folder
            '/home/understressengineer/programming/STEP_Program_SRM/dataset',  # Absolute path
        ]
        
        csv_files = []
        for folder in potential_folders:
            print(f"Looking for CSV files in: {folder}")
            found_files = get_csv_files(folder)
            if found_files:
                csv_files = found_files
                data_folder = folder
                print(f"✓ Found {len(csv_files)} CSV files in {folder}")
                break
        
        if not csv_files:
            print("❌ Could not find any CSV files in any of the potential locations.")
            print("Please create a dataset folder with CSV files or specify the correct path.")
            print("\nUsage example: python import_data.py --dataset /path/to/your/csv/files")
            
            # Ask if user wants to generate sample data
            response = input("\nWould you like to generate sample data for testing? (y/n): ")
            if response.lower() == 'y':
                generate_sample_data(connection)
            return
        
        print(f"Found {len(csv_files)} CSV files to import")
        for i, file in enumerate(csv_files):
            print(f"  {i+1}. {os.path.basename(file)}")
        
        # Define import order to respect foreign key constraints
        table_order = {
            'Screen': 1,
            'Movie': 2,
            'User': 3,
            'PaymentGateway': 4,
            'FoodItem': 5,
            'Seat': 6,
            'MovieCast': 7,
            'Review': 8,
            'Membership': 9,
            'Show': 10,
            'ShowSeat': 11,
            'FoodItemSize': 12,
            'Booking': 13,
            'Ticket': 14,
            'Payment': 15,
            'FoodOrder': 16,
            'FoodOrderItem': 17,
            'PointsTransaction': 18
        }
        
        # Group CSV files by target table
        csv_by_table = {}
        with tqdm(total=len(csv_files), desc="Mapping CSV files to tables") as pbar:
            for csv_file in csv_files:
                table_name = map_csv_to_table(csv_file)
                if table_name not in csv_by_table:
                    csv_by_table[table_name] = []
                csv_by_table[table_name].append(csv_file)
                pbar.update(1)
        
        # Sort tables by dependency order
        sorted_tables = sorted(csv_by_table.keys(), key=lambda t: table_order.get(t, 99))
        print("\nPlanned import order:")
        for i, table in enumerate(sorted_tables):
            print(f"  {i+1}. {table} - {len(csv_by_table[table])} files")
        
        # Process each table in dependency order
        with tqdm(total=len(sorted_tables), desc="Processing tables", position=0) as table_pbar:
            for table in sorted_tables:
                print(f"\n{'='*50}")
                print(f"Processing files for table: {table}")
                print(f"{'='*50}")
                
                success_count = 0
                with tqdm(total=len(csv_by_table[table]), desc=f"Files for {table}", position=1) as file_pbar:
                    for csv_file in csv_by_table[table]:
                        print(f"\nImporting {os.path.basename(csv_file)}")
                        if import_data(connection, csv_file, table):
                            success_count += 1
                        file_pbar.update(1)
                
                print(f"\n{table}: {success_count}/{len(csv_by_table[table])} files imported successfully")
                table_pbar.update(1)
            
        print("\n✅ Data import completed")
        
        # Final validation
        print("\nFinal table row counts:")
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        
        total_rows = 0
        for table in tables:
            count = count_table_rows(connection, table)
            total_rows += count
            print(f"  {table}: {count} rows")
        
        print(f"\nTotal rows in database: {total_rows}")
        if total_rows == 0:
            print("❌ WARNING: No data was imported to any table!")
        
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if connection:
            connection.close()
            print("Database connection closed")

def generate_sample_data(connection):
    """Generate sample data for testing"""
    try:
        print("Generating sample data...")
        
        # Generate Screen data
        screen_df = pd.DataFrame({
            'name': ['Screen A', 'Screen B', 'Screen C'],
            'class_type': ['Gold', 'Silver', 'Gold'],
            'capacity': [120, 80, 100]
        })
        
        # Generate Movie data
        movie_df = pd.DataFrame({
            'title': ['Interstellar', 'The Matrix', 'Inception'],
            'genre': ['Sci-Fi', 'Action', 'Thriller'],
            'rating': [8.6, 8.7, 8.8],
            'status': ['Now Showing', 'Coming Soon', 'Now Showing'],
            'poster_image_url': ['interstellar.jpg', 'matrix.jpg', 'inception.jpg']
        })
        
        # Generate User data
        user_df = pd.DataFrame({
            'name': ['John Doe', 'Jane Smith', 'Robert Brown'],
            'email': ['john@example.com', 'jane@example.com', 'robert@example.com'],
            'phone': ['1234567890', '0987654321', '1122334455']
        })
        
        # Create SQLAlchemy engine
        user = connection.user
        password = connection._password
        host = connection._host
        database = connection._database
        engine_url = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
        engine = create_engine(engine_url)
        
        # Import the sample data
        print("Importing Screen data...")
        screen_df.to_sql('Screen', con=engine, if_exists='append', index=False)
        
        print("Importing Movie data...")
        movie_df.to_sql('Movie', con=engine, if_exists='append', index=False)
        
        print("Importing User data...")
        user_df.to_sql('User', con=engine, if_exists='append', index=False)
        
        print("✅ Sample data generated and imported successfully")
        
    except Exception as e:
        print(f"Error generating sample data: {e}")

if __name__ == "__main__":
    main()