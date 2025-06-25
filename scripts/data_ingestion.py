"""
ETL Pipeline for Sales Analysis using DuckDB
Handles data ingestion, transformation, and loading into DuckDB for analysis.
"""

import duckdb
import pandas as pd
import json
from pathlib import Path
import logging
from typing import Dict, List, Optional
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SalesDataPipeline:
    """ETL pipeline for sales data analysis using DuckDB."""
    
    def __init__(self, db_path: str = ":memory:"):
        """Initialize the pipeline with DuckDB connection."""
        self.conn = duckdb.connect(db_path)
        self.data_dir = Path("data")
        
    def setup_database(self):
        """Create database schema and tables."""
        logger.info("Setting up database schema...")
        
        # Create sales table
        self.conn.execute("""
            CREATE OR REPLACE TABLE sales (
                transaction_id VARCHAR PRIMARY KEY,
                product_id VARCHAR,
                customer_id VARCHAR,
                sale_date DATE,
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                region VARCHAR,
                sales_rep VARCHAR
            )
        """)
        
        # Create products table
        self.conn.execute("""
            CREATE OR REPLACE TABLE products (
                product_id VARCHAR PRIMARY KEY,
                name VARCHAR,
                category VARCHAR,
                subcategory VARCHAR,
                brand VARCHAR,
                cost DECIMAL(10,2),
                price DECIMAL(10,2),
                margin DECIMAL(5,2)
            )
        """)
        
        # Create regions table
        self.conn.execute("""
            CREATE OR REPLACE TABLE regions (
                region_id VARCHAR PRIMARY KEY,
                region_name VARCHAR,
                country VARCHAR,
                timezone VARCHAR,
                manager VARCHAR,
                target_revenue DECIMAL(12,2)
            )
        """)
        
        logger.info("Database schema created successfully")
    
    def load_sales_data(self, file_path: str = "data/sales.csv"):
        """Load sales data from CSV into DuckDB."""
        logger.info(f"Loading sales data from {file_path}")
        
        try:
            # Use DuckDB's native CSV reader for better performance
            self.conn.execute(f"""
                INSERT INTO sales 
                SELECT * FROM read_csv_auto('{file_path}', header=true)
            """)
            
            count = self.conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
            logger.info(f"Loaded {count} sales records")
            
        except Exception as e:
            logger.error(f"Error loading sales data: {e}")
            raise
    
    def load_products_data(self, file_path: str = "data/products.json"):
        """Load products data from JSON into DuckDB."""
        logger.info(f"Loading products data from {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                products_data = json.load(f)
            
            # Convert to DataFrame for easier handling
            df = pd.json_normalize(products_data)
            
            # Register DataFrame as a view in DuckDB
            self.conn.register('products_temp', df)
            
            # Insert into products table
            self.conn.execute("""
                INSERT INTO products 
                SELECT * FROM products_temp
            """)
            
            count = self.conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            logger.info(f"Loaded {count} product records")
            
        except Exception as e:
            logger.error(f"Error loading products data: {e}")
            raise
    
    def load_regions_data(self, file_path: str = "data/regions.parquet"):
        """Load regions data from Parquet into DuckDB."""
        logger.info(f"Loading regions data from {file_path}")
        
        try:
            # DuckDB has native Parquet support
            self.conn.execute(f"""
                INSERT INTO regions 
                SELECT * FROM read_parquet('{file_path}')
            """)
            
            count = self.conn.execute("SELECT COUNT(*) FROM regions").fetchone()[0]
            logger.info(f"Loaded {count} region records")
            
        except Exception as e:
            logger.error(f"Error loading regions data: {e}")
            raise
    
    def create_derived_tables(self):
        """Create derived tables for analysis."""
        logger.info("Creating derived tables...")
        
        # Sales summary by month
        self.conn.execute("""
            CREATE OR REPLACE TABLE monthly_sales AS
            SELECT 
                EXTRACT(YEAR FROM sale_date) as year,
                EXTRACT(MONTH FROM sale_date) as month,
                region,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction_value,
                SUM(quantity) as total_quantity
            FROM sales 
            GROUP BY year, month, region
        """)
        
        # Product performance
        self.conn.execute("""
            CREATE OR REPLACE TABLE product_performance AS
            SELECT 
                p.product_id,
                p.name,
                p.category,
                p.brand,
                COUNT(s.transaction_id) as transaction_count,
                SUM(s.total_amount) as total_revenue,
                SUM(s.quantity) as total_quantity_sold,
                AVG(s.unit_price) as avg_selling_price,
                p.cost,
                (AVG(s.unit_price) - p.cost) / p.cost * 100 as margin_percent
            FROM products p
            LEFT JOIN sales s ON p.product_id = s.product_id
            GROUP BY p.product_id, p.name, p.category, p.brand, p.cost
        """)
        
        logger.info("Derived tables created successfully")
    
    def validate_data_quality(self):
        """Run data quality checks."""
        logger.info("Running data quality checks...")
        
        issues = []
        
        # Check for missing values in critical fields
        null_checks = [
            ("sales", "product_id"),
            ("sales", "customer_id"),
            ("sales", "total_amount"),
            ("products", "product_id"),
            ("products", "name")
        ]
        
        for table, column in null_checks:
            result = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
            """).fetchone()[0]
            
            if result > 0:
                issues.append(f"{table}.{column}: {result} null values")
        
        # Check for negative amounts
        negative_amounts = self.conn.execute("""
            SELECT COUNT(*) FROM sales WHERE total_amount < 0
        """).fetchone()[0]
        
        if negative_amounts > 0:
            issues.append(f"sales.total_amount: {negative_amounts} negative values")
        
        # Check for future dates
        future_dates = self.conn.execute("""
            SELECT COUNT(*) FROM sales WHERE sale_date > CURRENT_DATE
        """).fetchone()[0]
        
        if future_dates > 0:
            issues.append(f"sales.sale_date: {future_dates} future dates")
        
        if issues:
            logger.warning(f"Data quality issues found: {issues}")
        else:
            logger.info("All data quality checks passed")
        
        return issues
    
    def get_data_summary(self):
        """Get summary statistics of loaded data."""
        summary = {}
        
        tables = ["sales", "products", "regions"]
        for table in tables:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            summary[table] = count
        
        # Sales date range
        date_range = self.conn.execute("""
            SELECT MIN(sale_date), MAX(sale_date) FROM sales
        """).fetchone()
        
        summary["sales_date_range"] = f"{date_range[0]} to {date_range[1]}"
        
        # Total revenue
        total_revenue = self.conn.execute("""
            SELECT SUM(total_amount) FROM sales
        """).fetchone()[0]
        
        summary["total_revenue"] = total_revenue
        
        return summary
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline."""
        logger.info("Starting ETL pipeline...")
        
        try:
            # Setup
            self.setup_database()
            
            # Load data
            self.load_sales_data()
            self.load_products_data()
            
            # Try to load regions if file exists
            regions_file = Path("data/regions.parquet")
            if regions_file.exists():
                self.load_regions_data()
            else:
                logger.warning("regions.parquet not found, skipping regions data")
            
            # Create derived tables
            self.create_derived_tables()
            
            # Validate data
            self.validate_data_quality()
            
            # Summary
            summary = self.get_data_summary()
            logger.info(f"Pipeline completed successfully. Summary: {summary}")
            
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        self.conn.close()

def main():
    """Main function to run the ETL pipeline."""
    pipeline = SalesDataPipeline()
    
    try:
        summary = pipeline.run_pipeline()
        print("\n=== ETL Pipeline Summary ===")
        for key, value in summary.items():
            print(f"{key}: {value}")
    
    finally:
        pipeline.close()

if __name__ == "__main__":
    main()
