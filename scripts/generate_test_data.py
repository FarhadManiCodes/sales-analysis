#!/usr/bin/env python3
"""
Generate Test Data - Large files for performance testing
=======================================================

Creates large test datasets for comprehensive tmux layout testing.
Generates files that test data handling limits and performance.

Usage:
    python scripts/generate_test_data.py
    
Creates:
    - data/large_sales.csv (50MB+ for performance testing)
    - data/regions.parquet (Binary format testing)
    - Additional test files as needed
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random
from datetime import datetime, timedelta
import json


def generate_large_sales_data(output_file="data/large_sales.csv", target_rows=50000):
    """
    Generate large sales dataset for performance testing.
    Creates a file that tests data window limits and loading performance.
    """
    print(f"🔄 Generating large sales dataset: {target_rows:,} rows")
    
    # Seed for reproducible data
    np.random.seed(42)
    random.seed(42)
    
    # Base data for generation
    products = [f"PRD_{100 + i}" for i in range(20)]
    regions = ["North", "South", "East", "West", "Central"]
    channels = ["online", "retail", "wholesale", "mobile"]
    sales_reps = ["Alice Johnson", "Bob Chen", "Carol Davis", "David Wilson", "Eva Martinez", "Frank Lee"]
    
    # Generate data in chunks to manage memory
    chunk_size = 10000
    chunks = []
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = (end_date - start_date).days
    
    for chunk_start in range(0, target_rows, chunk_size):
        chunk_end = min(chunk_start + chunk_size, target_rows)
        chunk_rows = chunk_end - chunk_start
        
        print(f"  Generating chunk {chunk_start//chunk_size + 1}: rows {chunk_start:,} to {chunk_end:,}")
        
        # Generate chunk data
        chunk_data = {
            'transaction_id': [f"TXN_{str(i + chunk_start).zfill(8)}" for i in range(chunk_rows)],
            'date': [start_date + timedelta(days=random.randint(0, date_range)) for _ in range(chunk_rows)],
            'product_id': np.random.choice(products, chunk_rows),
            'customer_id': [f"CUST_{random.randint(1000, 9999)}" for _ in range(chunk_rows)],
            'quantity': np.random.poisson(3, chunk_rows) + 1,  # Poisson distribution, min 1
            'region': np.random.choice(regions, chunk_rows),
            'sales_rep': np.random.choice(sales_reps, chunk_rows),
            'channel': np.random.choice(channels, chunk_rows)
        }
        
        # Generate realistic prices based on product
        unit_prices = []
        for product in chunk_data['product_id']:
            base_price = hash(product) % 200 + 10  # Consistent price per product
            # Add some variation
            price_variation = np.random.normal(1.0, 0.1)
            final_price = round(base_price * price_variation, 2)
            unit_prices.append(max(final_price, 5.0))  # Minimum price $5
        
        chunk_data['unit_price'] = unit_prices
        chunk_data['total_amount'] = [
            round(q * p, 2) for q, p in zip(chunk_data['quantity'], chunk_data['unit_price'])
        ]
        
        # Create DataFrame chunk
        chunk_df = pd.DataFrame(chunk_data)
        chunks.append(chunk_df)
    
    # Combine all chunks
    print("📊 Combining chunks...")
    large_df = pd.concat(chunks, ignore_index=True)
    
    # Sort by date for realistic time series
    large_df = large_df.sort_values('date').reset_index(drop=True)
    
    # Create output directory if needed
    Path("data").mkdir(exist_ok=True)
    
    # Save to CSV
    print(f"💾 Saving to {output_file}...")
    large_df.to_csv(output_file, index=False)
    
    # Report statistics
    file_size = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"✅ Large sales dataset created!")
    print(f"   📁 File: {output_file}")
    print(f"   📏 Size: {file_size:.1f} MB")
    print(f"   📊 Rows: {len(large_df):,}")
    print(f"   📅 Date range: {large_df['date'].min()} to {large_df['date'].max()}")
    print(f"   💰 Total revenue: ${large_df['total_amount'].sum():,.2f}")
    
    return large_df


def generate_regions_parquet(output_file="data/regions.parquet"):
    """
    Generate regions data in Parquet format for binary format testing.
    """
    print(f"📦 Generating regions dataset (Parquet format)")
    
    regions_data = [
        {
            'region_code': 'North',
            'region_name': 'Northern Region',
            'country': 'USA',
            'state_province': 'Various Northern States',
            'population': 15000000,
            'gdp_per_capita': 55000.00,
            'market_potential': 0.85,
            'avg_income': 62000,
            'unemployment_rate': 3.2,
            'major_cities': ['Minneapolis', 'Detroit', 'Milwaukee'],
            'primary_industries': ['Manufacturing', 'Agriculture', 'Technology'],
            'climate_zone': 'Continental',
            'time_zone': 'Central/Eastern',
            'sales_tax_rate': 0.067,
            'business_friendliness_score': 8.2
        },
        {
            'region_code': 'South',
            'region_name': 'Southern Region',
            'country': 'USA',
            'state_province': 'Various Southern States',
            'population': 18000000,
            'gdp_per_capita': 48000.00,
            'market_potential': 0.78,
            'avg_income': 54000,
            'unemployment_rate': 4.1,
            'major_cities': ['Atlanta', 'Miami', 'Houston'],
            'primary_industries': ['Energy', 'Agriculture', 'Tourism'],
            'climate_zone': 'Subtropical',
            'time_zone': 'Central/Eastern',
            'sales_tax_rate': 0.074,
            'business_friendliness_score': 7.8
        },
        {
            'region_code': 'East',
            'region_name': 'Eastern Region',
            'country': 'USA',
            'state_province': 'Various Eastern States',
            'population': 22000000,
            'gdp_per_capita': 62000.00,
            'market_potential': 0.92,
            'avg_income': 68000,
            'unemployment_rate': 2.8,
            'major_cities': ['New York', 'Boston', 'Philadelphia'],
            'primary_industries': ['Finance', 'Technology', 'Healthcare'],
            'climate_zone': 'Temperate',
            'time_zone': 'Eastern',
            'sales_tax_rate': 0.082,
            'business_friendliness_score': 8.7
        },
        {
            'region_code': 'West',
            'region_name': 'Western Region',
            'country': 'USA',
            'state_province': 'Various Western States',
            'population': 20000000,
            'gdp_per_capita': 58000.00,
            'market_potential': 0.88,
            'avg_income': 65000,
            'unemployment_rate': 3.5,
            'major_cities': ['Los Angeles', 'San Francisco', 'Seattle'],
            'primary_industries': ['Technology', 'Entertainment', 'Aerospace'],
            'climate_zone': 'Various',
            'time_zone': 'Pacific',
            'sales_tax_rate': 0.089,
            'business_friendliness_score': 8.4
        },
        {
            'region_code': 'Central',
            'region_name': 'Central Region',
            'country': 'USA',
            'state_province': 'Various Central States',
            'population': 12000000,
            'gdp_per_capita': 52000.00,
            'market_potential': 0.75,
            'avg_income': 58000,
            'unemployment_rate': 3.8,
            'major_cities': ['Chicago', 'Denver', 'Kansas City'],
            'primary_industries': ['Agriculture', 'Manufacturing', 'Logistics'],
            'climate_zone': 'Continental',
            'time_zone': 'Central/Mountain',
            'sales_tax_rate': 0.069,
            'business_friendliness_score': 7.9
        }
    ]
    
    # Create DataFrame
    regions_df = pd.DataFrame(regions_data)
    
    # Create output directory if needed
    Path("data").mkdir(exist_ok=True)
    
    # Save as Parquet
    regions_df.to_parquet(output_file, index=False, engine='pyarrow')
    
    # Report statistics
    file_size = Path(output_file).stat().st_size / 1024
    print(f"✅ Regions dataset created!")
    print(f"   📁 File: {output_file}")
    print(f"   📏 Size: {file_size:.1f} KB")
    print(f"   📊 Rows: {len(regions_df)}")
    print(f"   🗂️  Format: Parquet (binary)")
    
    return regions_df


def generate_complex_json(output_file="data/complex_data.json"):
    """
    Generate complex nested JSON for JSON parsing testing.
    """
    print(f"📋 Generating complex JSON dataset")
    
    complex_data = {
        "metadata": {
            "version": "1.0",
            "generated_at": datetime.now().isoformat(),
            "description": "Complex nested JSON for testing",
            "schema_version": "2.1"
        },
        "sales_summary": {
            "total_revenue": 1250000.50,
            "total_transactions": 45678,
            "period": {
                "start_date": "2023-01-01",
                "end_date": "2024-12-31"
            },
            "by_region": [
                {
                    "region": "North",
                    "metrics": {
                        "revenue": 350000.25,
                        "transactions": 12500,
                        "avg_transaction": 28.00,
                        "top_products": [
                            {"product_id": "PRD_101", "name": "Widget A", "sales": 5000},
                            {"product_id": "PRD_102", "name": "Widget B", "sales": 3200}
                        ]
                    }
                },
                {
                    "region": "South",
                    "metrics": {
                        "revenue": 280000.75,
                        "transactions": 10200,
                        "avg_transaction": 27.45,
                        "top_products": [
                            {"product_id": "PRD_103", "name": "Gadget X", "sales": 4100},
                            {"product_id": "PRD_104", "name": "Gadget Y", "sales": 2800}
                        ]
                    }
                }
            ]
        },
        "customer_segments": {
            "high_value": {
                "count": 1250,
                "avg_spend": 580.25,
                "characteristics": {
                    "age_range": "35-55",
                    "income_bracket": "high",
                    "purchase_frequency": "monthly"
                }
            },
            "medium_value": {
                "count": 4800,
                "avg_spend": 240.50,
                "characteristics": {
                    "age_range": "25-45",
                    "income_bracket": "medium",
                    "purchase_frequency": "quarterly"
                }
            }
        },
        "product_performance": {
            "categories": {
                "electronics": {
                    "total_revenue": 750000,
                    "units_sold": 25000,
                    "avg_rating": 4.2,
                    "return_rate": 0.032,
                    "seasonal_trends": [
                        {"month": "Q1", "sales_multiplier": 0.8},
                        {"month": "Q2", "sales_multiplier": 1.1},
                        {"month": "Q3", "sales_multiplier": 0.9},
                        {"month": "Q4", "sales_multiplier": 1.4}
                    ]
                }
            }
        }
    }
    
    # Create output directory if needed
    Path("data").mkdir(exist_ok=True)
    
    # Save as JSON
    with open(output_file, 'w') as f:
        json.dump(complex_data, f, indent=2)
    
    # Report statistics
    file_size = Path(output_file).stat().st_size / 1024
    print(f"✅ Complex JSON dataset created!")
    print(f"   📁 File: {output_file}")
    print(f"   📏 Size: {file_size:.1f} KB")
    print(f"   🗂️  Format: JSON (nested)")
    
    return complex_data


def main():
    """Generate all test datasets."""
    print("🏗️  Test Data Generation for Tmux Analysis Layout")
    print("=" * 60)
    
    try:
        # Generate large sales data for performance testing
        large_sales = generate_large_sales_data(target_rows=50000)
        print()
        
        # Generate regions parquet data
        regions = generate_regions_parquet()
        print()
        
        # Generate complex JSON data
        complex_json = generate_complex_json()
        print()
        
        print("🎉 All test datasets generated successfully!")
        print()
        print("📋 Generated files:")
        print("   • data/large_sales.csv - Large dataset for performance testing")
        print("   • data/regions.parquet - Binary format testing")
        print("   • data/complex_data.json - Complex JSON structure testing")
        print()
        print("💡 Next steps:")
        print("   1. Use 'fdata' in Data window to explore these files")
        print("   2. Test 'quickload()' function with large_sales.csv")
        print("   3. Test DuckDB performance with read_csv_auto() on large file")
        print("   4. Test JSON structure exploration with complex_data.json")
        print("   5. Test Parquet loading in both pandas and DuckDB")
        
        return True
        
    except Exception as e:
        print(f"❌ Data generation failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
