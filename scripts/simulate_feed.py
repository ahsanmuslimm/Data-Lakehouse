import os
import csv
import json
import random
import argparse
from datetime import datetime, timedelta
from io import StringIO
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_seed_data(seed_path, column_index):
    """Loads valid dimension values from a seed CSV."""
    values = []
    try:
        with open(seed_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader) # skip header
            for row in reader:
                if len(row) > column_index:
                    val = row[column_index].strip()
                    if val and val != 'Unknown':
                        values.append(val)
    except FileNotFoundError:
        print(f"Warning: Seed file not found at {seed_path}")
    return values

def get_next_order_id(counter_file, count):
    """Gets the next starting order ID and updates the counter file."""
    start_id = 1
    if os.path.exists(counter_file):
        with open(counter_file, 'r') as f:
            data = json.load(f)
            start_id = data.get('last_order_id', 0) + 1
    
    with open(counter_file, 'w') as f:
        json.dump({'last_order_id': start_id + count - 1}, f)
        
    return start_id

def generate_records(num_records, order_date, dimensions, start_order_id):
    """Generates a list of simulated sales records."""
    records = []
    
    countries = dimensions.get('Country', ['Unknown'])
    regions = dimensions.get('Region', ['Unknown'])
    branches = dimensions.get('Branch', ['Unknown'])
    item_types = dimensions.get('Item_Type', ['Unknown'])
    categories = dimensions.get('Category', ['Unknown'])
    sales_channels = dimensions.get('Sales_Channel', ['Unknown'])
    
    order_date_obj = datetime.strptime(order_date, '%Y-%m-%d')
    
    for i in range(num_records):
        order_id = start_order_id + i
        
        # Simulate Ship Date (0 to 14 days after Order Date)
        ship_delay = random.randint(0, 14)
        ship_date = (order_date_obj + timedelta(days=ship_delay)).strftime('%Y-%m-%d')
        
        unit_price = round(random.uniform(5.00, 1000.00), 2)
        # Simulate Unit Cost as a percentage of Unit Price to ensure profitability
        unit_cost = round(unit_price * random.uniform(0.3, 0.8), 2)
        
        # Region,Country,Item Type,Sales Channel,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,Branch,Category
        # We include Branch and Category as well, even if stg model only parses first 10 for now.
        # Actually, let's format it exactly as a raw line:
        record = [
            random.choice(regions),             # Region
            random.choice(countries),           # Country
            random.choice(item_types),          # Item Type
            random.choice(sales_channels),      # Sales Channel
            order_date,                         # Order Date
            str(order_id),                      # Order ID
            ship_date,                          # Ship Date
            str(random.randint(100, 10000)),    # Units Sold
            f"{unit_price:.2f}",                # Unit Price
            f"{unit_cost:.2f}",                 # Unit Cost
            random.choice(branches),            # Branch
            random.choice(categories)           # Category
        ]
        records.append(record)
        
    return records

def upload_to_minio(csv_buffer, filename):
    """Uploads a file-like buffer to MinIO."""
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'changeme')
    bucket = os.getenv('MINIO_SOURCE_BUCKET', 'source')
    
    # Ensure endpoint includes http if missing for boto3
    if not endpoint.startswith('http'):
        endpoint_url = f"http://{endpoint}"
    else:
        endpoint_url = endpoint
        
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1' # dummy region
    )
    
    # Create bucket if it doesn't exist (optional but good for safety)
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        # Simplified: assume it exists or fail
        pass
        
    print(f"Uploading {filename} to MinIO bucket '{bucket}'...")
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=csv_buffer.getvalue()
    )
    print("Upload complete.")

def main():
    parser = argparse.ArgumentParser(description="Simulate Retail Sales Data Feed")
    parser.add_argument('--date', type=str, default=datetime.now().strftime('%Y-%m-%d'), help="Order Date (YYYY-MM-DD)")
    parser.add_argument('--records', type=int, default=150, help="Number of records to generate")
    args = parser.parse_args()
    
    # Base paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    seeds_dir = os.path.join(project_root, 'dbt', 'seeds')
    counter_file = os.path.join(script_dir, 'counter.json')
    
    # Load dimensions from seeds
    # The column indices are based on standard seed structures: 
    # Country(C_ID,Country), Region(R_ID,Region), Branch(B_ID,B_Name), 
    # Product(I_ID,I_Type), Category(C_ID,C_Name), Channel(S_ID,S_Channel)
    dimensions = {
        'Country': load_seed_data(os.path.join(seeds_dir, 'country.csv'), 1),
        'Region': load_seed_data(os.path.join(seeds_dir, 'region.csv'), 1),
        'Branch': load_seed_data(os.path.join(seeds_dir, 'branch.csv'), 1),
        'Item_Type': load_seed_data(os.path.join(seeds_dir, 'product.csv'), 1),
        'Category': load_seed_data(os.path.join(seeds_dir, 'category.csv'), 1),
        'Sales_Channel': load_seed_data(os.path.join(seeds_dir, 'channel.csv'), 1)
    }
    
    # Verify dimensions loaded
    for dim, values in dimensions.items():
        if not values:
            print(f"Warning: No values loaded for dimension '{dim}'. Defaulting to 'Unknown'.")
            dimensions[dim] = ['Unknown']
            
    # Get sequence block
    start_id = get_next_order_id(counter_file, args.records)
    
    # Generate records
    print(f"Generating {args.records} records for {args.date}...")
    records = generate_records(args.records, args.date, dimensions, start_id)
    
    # Write to CSV buffer
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    # We do not write a header, as raw_line expects just data, but let's check
    # Actually, often CSVs have headers. The prompt just says "Generate N records". 
    # Let's write them without a header since stg_bronze_sales parses raw_line blindly. 
    # We'll just write rows directly.
    for row in records:
        writer.writerow(row)
        
    # Upload to MinIO
    filename = f"source/sales_{args.date}.csv"
    upload_to_minio(csv_buffer, filename)
    
if __name__ == "__main__":
    main()
