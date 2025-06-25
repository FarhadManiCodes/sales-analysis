"""
Generate missing test data files for sales analysis project.
Creates regions.parquet and large_sales.csv for testing purposes.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_regions_data():
    """Generate regions.parquet file with realistic regional data."""
    logger.info("Generating regions data...")
    
    regions_data = [
        {
            "region_id": "NA_EAST",
            "region_name": "North America East",
            "country": "United States",
            "timezone": "America/New_York",
            "manager": "Sarah Johnson",
            "target_revenue": 2500000.00
        },
        {
            "region_id": "NA_WEST", 
            "region_name": "North America West",
            "country": "United States",
            "timezone": "America/Los_Angeles",
            "manager": "Michael Chen",
            "target_revenue": 2800000.00
        },
        {
            "region_id": "NA_CENTRAL",
            "region_name": "North America Central", 
            "country": "United States",
            "timezone": "America/Chicago",
            "manager": "David Rodriguez",
            "target_revenue": 2200000.00
        },
        {
            "region_id": "EU_NORTH",
            "region_name": "Europe North",
            "country": "Multiple",
            "timezone": "Europe/Stockholm", 
            "manager": "Anna Larsson",
            "target_revenue": 1800000.00
        },
        {
            "region_id": "EU_CENTRAL",
            "region_name": "Europe Central",
            "country": "Multiple", 
            "timezone": "Europe/Berlin",
            "manager": "Klaus Mueller",
            "target_revenue": 2100000.00
        },
        {
            "region_id": "EU_SOUTH",
            "region_name": "Europe South",
            "country": "Multiple",
            "timezone": "Europe/Rome",
            "manager": "Maria Rossi",
            "target_revenue": 1600000.00
        },
        {
            "region_id": "APAC_EAST",
            "region_name": "Asia Pacific East",
            "country": "Multiple",
            "timezone": "Asia/Tokyo",
            "manager": "Hiroshi Tanaka",
            "target_revenue": 2000000.00
        },
        {
            "region_id": "APAC_SOUTH",
            "region_name": "Asia Pacific South",
            "country": "Multiple", 
            "timezone": "Asia/Singapore",
            "manager": "Li Wei",
            "target_revenue": 1900000.00
        },
        {
            "region_id": "LATAM",
            "region_name": "Latin America",
            "country": "Multiple",
            "timezone": "America/Sao_Paulo", 
            "manager": "Carlos Silva",
            "target_revenue": 1400000.00
        },
        {
            "region_id": "MEA",
            "region_name": "Middle East & Africa",
            "country": "Multiple",
            "timezone": "Africa/Johannesburg",
            "manager": "Ahmed Hassan", 
            "target_revenue": 1200000.00
        }
    ]
    
    df = pd.DataFrame(regions_data)
    
    # Save as parquet
    output_path = Path("data/regions.parquet")
    output_path.parent.mkdir(exist_ok=True)
    df.to_parquet(output_path, index=False)
    
    logger.info(f"Created {output_path} with {len(df)} regions")
    return df

def generate_large_sales_data(num_records: int = 50000):
    """Generate large_sales.csv for performance testing."""
    logger.info(f"Generating large sales data with {num_records} records...")
    
    # Read existing sales data to get realistic patterns
    existing_sales_path = Path("data/sales.csv")
    if existing_sales_path.exists():
        existing_df = pd.read_csv(existing_sales_path)
        
        # Extract patterns from existing data
        existing_products = existing_df['product_id'].unique() if 'product_id' in existing_df.columns else [f"PROD_{i:04d}" for i in range(1, 101)]
        existing_customers = existing_df['customer_id'].unique() if 'customer_id' in existing_df.columns else [f"CUST_{i:05d}" for i in range(1, 1001)]
        existing_regions = existing_df['region'].unique() if 'region' in existing_df.columns else ["North", "South", "East", "West", "Central"]
        
        # Get price ranges
        if 'unit_price' in existing_df.columns:
            min_price = existing_df['unit_price'].min()
            max_price = existing_df['unit_price'].max()
        else:
            min_price, max_price = 10.0, 500.0
            
    else:
        # Default values if no existing sales data
        existing_products = [f"PROD_{i:04d}" for i in range(1, 201)]
        existing_customers = [f"CUST_{i:05d}" for i in range(1, 2001)] 
        existing_regions = ["NA_EAST", "NA_WEST", "NA_CENTRAL", "EU_NORTH", "EU_CENTRAL", "EU_SOUTH", "APAC_EAST", "APAC_SOUTH", "LATAM", "MEA"]
        min_price, max_price = 10.0, 500.0
    
    # Sales representatives
    sales_reps = [
        "Alice Cooper", "Bob Wilson", "Carol Davis", "Dan Brown", "Eva Martinez",
        "Frank Thompson", "Grace Lee", "Henry Wang", "Isabel Garcia", "Jack Smith",
        "Kate Johnson", "Liam O'Connor", "Maya Patel", "Noah Kim", "Olivia Zhang",
        "Paul Anderson", "Quinn Taylor", "Rachel Green", "Sam Miller", "Tina Liu"
    ]
    
    # Generate date range (last 2 years)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=730)
    
    # Generate synthetic data
    np.random.seed(42)  # For reproducible results
    random.seed(42)
    
    data = []
    
    for i in range(num_records):
        # Generate transaction ID
        transaction_id = f"TXN_{i+100000:06d}"
        
        # Random date with some seasonality (more sales in Nov-Dec)
        days_back = np.random.exponential(365) % 730
        sale_date = end_date - timedelta(days=int(days_back))
        
        # Add holiday season boost
        month = sale_date.month
        if month in [11, 12]:  # Holiday season
            quantity_multiplier = 1.5
        elif month in [6, 7, 8]:  # Summer
            quantity_multiplier = 1.2
        else:
            quantity_multiplier = 1.0
        
        # Random product and customer
        product_id = np.random.choice(existing_products)
        customer_id = np.random.choice(existing_customers)
        region = np.random.choice(existing_regions)
        sales_rep = np.random.choice(sales_reps)
        
        # Generate quantity (1-10, weighted toward smaller quantities)
        base_quantity = np.random.choice([1, 1, 1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        quantity = max(1, int(base_quantity * quantity_multiplier))
        
        # Generate unit price with some product consistency
        np.random.seed(hash(product_id) % 1000)  # Consistent pricing per product
        unit_price = round(np.random.uniform(min_price, max_price), 2)
        np.random.seed()  # Reset seed
        
        # Calculate total amount
        total_amount = round(quantity * unit_price, 2)
        
        data.append({
            "transaction_id": transaction_id,
            "product_id": product_id,
            "customer_id": customer_id,
            "sale_date": sale_date,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "region": region,
            "sales_rep": sales_rep
        })
        
        # Progress indicator
        if (i + 1) % 10000 == 0:
            logger.info(f"Generated {i + 1:,} records...")
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Sort by date for better compression and analysis
    df = df.sort_values('sale_date')
    
    # Save as CSV
    output_path = Path("data/large_sales.csv")
    output_path.parent.mkdir(exist_ok=True)
    df.to_csv(output_path, index=False)
    
    # Print summary statistics
    logger.info(f"Created {output_path}")
    logger.info(f"Records: {len(df):,}")
    logger.info(f"Date range: {df['sale_date'].min()} to {df['sale_date'].max()}")
    logger.info(f"Total revenue: ${df['total_amount'].sum():,.2f}")
    logger.info(f"Unique customers: {df['customer_id'].nunique():,}")
    logger.info(f"Unique products: {df['product_id'].nunique():,}")
    logger.info(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return df

def generate_additional_products(num_products: int = 50):
    """Generate additional products to supplement existing products.json."""
    logger.info(f"Generating {num_products} additional products...")
    
    categories = [
        "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
        "Toys", "Health & Beauty", "Automotive", "Tools", "Food & Beverage"
    ]
    
    subcategories = {
        "Electronics": ["Smartphones", "Laptops", "Tablets", "Accessories", "Audio"],
        "Clothing": ["Shirts", "Pants", "Dresses", "Shoes", "Accessories"],
        "Home & Garden": ["Furniture", "Decor", "Appliances", "Garden Tools", "Lighting"],
        "Sports": ["Fitness Equipment", "Outdoor Gear", "Team Sports", "Water Sports", "Winter Sports"],
        "Books": ["Fiction", "Non-Fiction", "Educational", "Children's", "Reference"],
        "Toys": ["Action Figures", "Board Games", "Educational", "Electronic", "Outdoor"],
        "Health & Beauty": ["Skincare", "Makeup", "Hair Care", "Supplements", "Personal Care"],
        "Automotive": ["Parts", "Accessories", "Tools", "Electronics", "Maintenance"],
        "Tools": ["Hand Tools", "Power Tools", "Hardware", "Measuring", "Safety"],
        "Food & Beverage": ["Snacks", "Beverages", "Ingredients", "Organic", "International"]
    }
    
    brands = [
        "TechPro", "StyleMax", "HomeComfort", "SportElite", "BookWorld",
        "PlayTime", "BeautyPlus", "AutoMax", "ToolCraft", "FreshTaste"
    ]
    
    products = []
    
    for i in range(num_products):
        category = np.random.choice(categories)
        subcategory = np.random.choice(subcategories[category])
        brand = np.random.choice(brands)
        
        # Generate realistic pricing
        if category == "Electronics":
            cost = round(np.random.uniform(50, 800), 2)
        elif category in ["Automotive", "Tools"]:
            cost = round(np.random.uniform(20, 300), 2)
        elif category in ["Clothing", "Health & Beauty"]:
            cost = round(np.random.uniform(10, 150), 2)
        else:
            cost = round(np.random.uniform(5, 100), 2)
        
        # Price with margin (20-60%)
        margin = np.random.uniform(0.2, 0.6)
        price = round(cost * (1 + margin), 2)
        
        product_id = f"PROD_{i+1000:04d}"
        name = f"{brand} {subcategory} {np.random.choice(['Pro', 'Elite', 'Classic', 'Premium', 'Basic'])}"
        
        products.append({
            "product_id": product_id,
            "name": name,
            "category": category,
            "subcategory": subcategory,
            "brand": brand,
            "cost": cost,
            "price": price,
            "margin": round(margin * 100, 2)
        })
    
    # Save as JSON
    output_path = Path("data/additional_products.json")
    pd.DataFrame(products).to_json(output_path, orient="records", indent=2)
    
    logger.info(f"Created {output_path} with {len(products)} additional products")
    return products

def main():
    """Generate all missing data files."""
    logger.info("Starting data generation process...")
    
    # Create data directory
    Path("data").mkdir(exist_ok=True)
    
    # Generate regions data
    regions_df = generate_regions_data()
    
    # Generate large sales data
    large_sales_df = generate_large_sales_data(50000)
    
    # Generate additional products
    additional_products = generate_additional_products(100)
    
    logger.info("Data generation completed successfully!")
    
    # Summary
    print("\n=== Generated Files Summary ===")
    print(f"✓ regions.parquet: {len(regions_df)} regions")
    print(f"✓ large_sales.csv: {len(large_sales_df):,} transactions")
    print(f"✓ additional_products.json: {len(additional_products)} products")
    print(f"\nTotal estimated revenue in large dataset: ${large_sales_df['total_amount'].sum():,.2f}")

if __name__ == "__main__":
    main()
