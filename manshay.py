import psycopg2
import csv
import os
import time
from datetime import datetime

# Simple Configuration
CONFIG = {
    'csv_path': 'F:\\data_insertion\\CIRCUIT_STITCHING_REPORT_DETAIL_202506110930801.csv',
    'table_name': 'loan_data',
    'chunk_size': 50000,  # Process 50K rows at a time
    'db_config': {
        'host': 'localhost',
        'database': 'postgres', 
        'user': 'postgres',
        'password': 'abhi',
        'port': '5432'
    }
}

def get_file_info():
    """Get basic file information"""
    if not os.path.exists(CONFIG['csv_path']):
        raise FileNotFoundError(f"File not found: {CONFIG['csv_path']}")
    
    file_size = os.path.getsize(CONFIG['csv_path'])
    print(f"File size: {file_size:,} bytes ({file_size/1024/1024/1024:.2f} GB)")
    return file_size

def get_csv_headers():
    """Get CSV headers and detect structure"""
    print("Reading CSV headers...")
    
    # Try different encodings for large files
    encodings = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(CONFIG['csv_path'], 'r', encoding=encoding, newline='') as f:
                # Read small sample to detect delimiter
                sample = f.read(1024)
                f.seek(0)
                
                # Detect delimiter
                if ',' in sample and sample.count(',') > sample.count('\t'):
                    delimiter = ','
                elif '\t' in sample:
                    delimiter = '\t'
                else:
                    delimiter = ','
                
                # Get headers
                reader = csv.reader(f, delimiter=delimiter)
                headers = next(reader)
                
                print(f"✓ Encoding: {encoding}")
                print(f"✓ Delimiter: '{delimiter}'")
                print(f"✓ Columns: {len(headers)}")
                
                return headers, delimiter, encoding
                
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error with {encoding}: {e}")
            continue
    
    raise ValueError("Could not read CSV file with any encoding")

def clean_column_name(name):
    """Clean column names for PostgreSQL"""
    if not name or str(name).strip() == '':
        return 'col_unnamed'
    
    # Simple cleaning
    clean = str(name).strip().lower()
    clean = ''.join(c if c.isalnum() else '_' for c in clean)
    
    # Don't start with number
    if clean and clean[0].isdigit():
        clean = f'col_{clean}'
    
    return clean or 'col_unnamed'

def create_table(headers):
    """Create table with simple structure"""
    print("Creating table...")
    
    conn = psycopg2.connect(**CONFIG['db_config'])
    cursor = conn.cursor()
    
    try:
        # Clean column names
        clean_headers = []
        seen = set()
        
        for i, header in enumerate(headers):
            clean = clean_column_name(header)
            if clean in seen:
                clean = f"{clean}_{i}"
            seen.add(clean)
            clean_headers.append(clean)
        
        # Drop and create table
        cursor.execute(f"DROP TABLE IF EXISTS {CONFIG['table_name']}")
        
        columns = ', '.join([f'"{col}" TEXT' for col in clean_headers])
        sql = f"""
        CREATE TABLE {CONFIG['table_name']} (
            id BIGSERIAL PRIMARY KEY,
            {columns},
            import_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(sql)
        conn.commit()
        
        print(f"✓ Table created with {len(clean_headers)} columns")
        return clean_headers
        
    finally:
        cursor.close()
        conn.close()

def process_chunk(chunk_data, headers, chunk_num):
    """Process a single chunk of data"""
    if not chunk_data:
        return 0
    
    conn = psycopg2.connect(**CONFIG['db_config'])
    cursor = conn.cursor()
    
    try:
        # Prepare bulk insert
        columns = ', '.join([f'"{col}"' for col in headers])
        placeholders = ', '.join(['%s'] * len(headers))
        sql = f"INSERT INTO {CONFIG['table_name']} ({columns}) VALUES ({placeholders})"
        
        # Clean and prepare data
        clean_data = []
        for row in chunk_data:
            clean_row = []
            for i in range(len(headers)):
                if i < len(row):
                    val = str(row[i]).strip() if row[i] is not None else ''
                    # Handle empty/null values
                    if val in ['', 'NULL', 'NA', 'N/A']:
                        val = None
                    clean_row.append(val)
                else:
                    clean_row.append(None)
            clean_data.append(clean_row)
        
        # Bulk insert
        cursor.executemany(sql, clean_data)
        conn.commit()
        
        return len(clean_data)
        
    except Exception as e:
        print(f"✗ Chunk {chunk_num} failed: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def import_large_csv():
    """Main function to import large CSV in chunks"""
    start_time = time.time()
    
    try:
        print("=" * 60)
        print("LARGE CSV IMPORT - CHUNK PROCESSING")
        print("=" * 60)
        
        # Step 1: File info
        file_size = get_file_info()
        
        # Step 2: Get headers
        headers, delimiter, encoding = get_csv_headers()
        
        # Step 3: Create table
        clean_headers = create_table(headers)
        
        # Step 4: Process file in chunks
        print(f"\nProcessing file in chunks of {CONFIG['chunk_size']:,} rows...")
        
        total_rows = 0
        chunk_num = 0
        
        with open(CONFIG['csv_path'], 'r', encoding=encoding, newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            next(reader)  # Skip header
            
            chunk_data = []
            
            for row_num, row in enumerate(reader, 1):
                chunk_data.append(row)
                
                # Process chunk when full
                if len(chunk_data) >= CONFIG['chunk_size']:
                    chunk_num += 1
                    processed = process_chunk(chunk_data, clean_headers, chunk_num)
                    total_rows += processed
                    
                    elapsed = time.time() - start_time
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    
                    print(f"Chunk {chunk_num:3d}: {processed:6,} rows | "
                          f"Total: {total_rows:8,} | Rate: {rate:6,.0f} rows/sec")
                    
                    chunk_data = []
            
            # Process final chunk
            if chunk_data:
                chunk_num += 1
                processed = process_chunk(chunk_data, clean_headers, chunk_num)
                total_rows += processed
                print(f"Chunk {chunk_num:3d}: {processed:6,} rows | "
                      f"Total: {total_rows:8,} | FINAL")
        
        # Final results
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("IMPORT COMPLETED!")
        print(f"Total rows imported: {total_rows:,}")
        print(f"Total time: {elapsed:.1f} seconds")
        print(f"Average rate: {total_rows/elapsed:,.0f} rows/second")
        print(f"Data size processed: {file_size/1024/1024:.1f} MB")
        print("=" * 60)
        
        # Create simple index for performance
        print("Creating index...")
        conn = psycopg2.connect(**CONFIG['db_config'])
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{CONFIG['table_name']}_id ON {CONFIG['table_name']} (id)")
            conn.commit()
            print("✓ Index created")
        except Exception as e:
            print(f"Index creation failed: {e}")
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"\n✗ IMPORT FAILED: {e}")
        raise

if __name__ == '__main__':
    import_large_csv()