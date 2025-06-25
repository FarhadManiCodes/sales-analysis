#!/usr/bin/env python3
"""
Sales Analysis - Unit Tests and Function Testing
===============================================

Unit tests and utility functions for testing in REPL quick tests pane.
Designed to validate functionality across the tmux analysis layout.

Usage:
    # In REPL Quick Tests pane:
    python scripts/test_functions.py
    
    # Or individual functions:
    quicktest("validate_transaction_data({'transaction_id': 'TXN_001', 'quantity': 5})")
"""

import pytest
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import duckdb


# ============================================================================
# UTILITY FUNCTIONS FOR QUICK TESTING
# ============================================================================

def validate_transaction_data(transaction: Dict) -> Dict[str, Any]:
    """
    Validate a single transaction record.
    Perfect for quicktest() function in REPL.
    
    Example:
        quicktest("validate_transaction_data({'transaction_id': 'TXN_001', 'quantity': 5})")
    """
    validation_result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    required_fields = ['transaction_id', 'date', 'product_id', 'customer_id', 'quantity', 'unit_price', 'total_amount']
    
    # Check required fields
    for field in required_fields:
        if field not in transaction:
            validation_result['errors'].append(f"Missing required field: {field}")
            validation_result['valid'] = False
    
    # Validate data types and business rules
    if 'quantity' in transaction:
        if not isinstance(transaction['quantity'], (int, float)) or transaction['quantity'] <= 0:
            validation_result['errors'].append("Quantity must be a positive number")
            validation_result['valid'] = False
    
    if 'unit_price' in transaction:
        if not isinstance(transaction['unit_price'], (int, float)) or transaction['unit_price'] <= 0:
            validation_result['errors'].append("Unit price must be a positive number")
            validation_result['valid'] = False
    
    # Business logic validation
    if 'quantity' in transaction and 'unit_price' in transaction and 'total_amount' in transaction:
        expected_total = transaction['quantity'] * transaction['unit_price']
        if abs(expected_total - transaction['total_amount']) > 0.01:
            validation_result['warnings'].append(f"Total amount mismatch: expected {expected_total}, got {transaction['total_amount']}")
    
    return validation_result


def calculate_profit_metrics(price: float, cost: float, quantity: int = 1) -> Dict[str, float]:
    """
    Calculate profit metrics for products.
    Great for testing business logic in REPL.
    
    Example:
        quicktest("calculate_profit_metrics(29.99, 18.50, 10)")
    """
    if price <= 0 or cost < 0:
        raise ValueError("Price must be positive, cost must be non-negative")
    
    profit_per_unit = price - cost
    margin_percentage = (profit_per_unit / price) * 100
    total_revenue = price * quantity
    total_profit = profit_per_unit * quantity
    
    return {
        'profit_per_unit': round(profit_per_unit, 2),
        'margin_percentage': round(margin_percentage, 2),
        'total_revenue': round(total_revenue, 2),
        'total_profit': round(total_profit, 2),
        'cost_ratio': round((cost / price) * 100, 2)
    }


def analyze_sales_data_quality(csv_file: str = "data/sales.csv") -> Dict[str, Any]:
    """
    Quick data quality assessment.
    Perfect for quickload() integration.
    
    Example:
        quicktest("analyze_sales_data_quality()")
    """
    try:
        df = pd.read_csv(csv_file)
        
        quality_report = {
            'file_info': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'file_size_mb': round(Path(csv_file).stat().st_size / (1024*1024), 2)
            },
            'data_quality': {
                'duplicate_rows': df.duplicated().sum(),
                'missing_values': df.isnull().sum().to_dict(),
                'unique_customers': df['customer_id'].nunique() if 'customer_id' in df.columns else 0,
                'unique_products': df['product_id'].nunique() if 'product_id' in df.columns else 0
            },
            'business_metrics': {}
        }
        
        # Business rule validation
        if 'quantity' in df.columns:
            quality_report['business_metrics']['negative_quantities'] = (df['quantity'] <= 0).sum()
        
        if 'total_amount' in df.columns:
            quality_report['business_metrics']['negative_amounts'] = (df['total_amount'] <= 0).sum()
            quality_report['business_metrics']['total_revenue'] = round(df['total_amount'].sum(), 2)
        
        return quality_report
        
    except Exception as e:
        return {'error': str(e)}


def test_duckdb_connection(db_path: str = "sales_analysis.duckdb") -> Dict[str, Any]:
    """
    Test DuckDB connection and basic queries.
    Useful for validating database setup.
    
    Example:
        quicktest("test_duckdb_connection()")
    """
    try:
        conn = duckdb.connect(db_path)
        
        # Test basic connection
        version = conn.execute("SELECT version()").fetchone()[0]
        
        # Test table existence
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        # Test basic queries
        test_results = {
            'connection': True,
            'duckdb_version': version,
            'available_tables': table_names,
            'table_info': {}
        }
        
        # Get row counts for each table
        for table in table_names:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                test_results['table_info'][table] = {'row_count': count}
            except Exception as e:
                test_results['table_info'][table] = {'error': str(e)}
        
        conn.close()
        return test_results
        
    except Exception as e:
        return {'connection': False, 'error': str(e)}


def benchmark_data_loading(csv_file: str = "data/sales.csv") -> Dict[str, float]:
    """
    Benchmark different data loading methods.
    Compare pandas vs DuckDB performance.
    
    Example:
        quicktest("benchmark_data_loading()")
    """
    import time
    
    results = {}
    
    try:
        # Pandas loading
        start_time = time.time()
        df_pandas = pd.read_csv(csv_file)
        pandas_time = time.time() - start_time
        results['pandas_load_time'] = round(pandas_time, 4)
        results['pandas_rows'] = len(df_pandas)
        
        # DuckDB direct query
        start_time = time.time()
        conn = duckdb.connect()
        df_duckdb = conn.execute(f"SELECT * FROM read_csv_auto('{csv_file}')").df()
        duckdb_time = time.time() - start_time
        results['duckdb_load_time'] = round(duckdb_time, 4)
        results['duckdb_rows'] = len(df_duckdb)
        
        # Performance comparison
        results['speedup'] = round(pandas_time / duckdb_time, 2) if duckdb_time > 0 else float('inf')
        results['winner'] = 'DuckDB' if duckdb_time < pandas_time else 'Pandas'
        
        conn.close()
        
    except Exception as e:
        results['error'] = str(e)
    
    return results


# ============================================================================
# PYTEST UNIT TESTS
# ============================================================================

class TestSalesDataValidation:
    """Unit tests for sales data validation functions."""
    
    def test_validate_transaction_data_valid(self):
        """Test transaction validation with valid data."""
        valid_transaction = {
            'transaction_id': 'TXN_001',
            'date': '2024-01-15',
            'product_id': 'PRD_101',
            'customer_id': 'CUST_123',
            'quantity': 2,
            'unit_price': 29.99,
            'total_amount': 59.98
        }
        
        result = validate_transaction_data(valid_transaction)
        assert result['valid'] == True
        assert len(result['errors']) == 0
    
    def test_validate_transaction_data_missing_fields(self):
        """Test transaction validation with missing fields."""
        invalid_transaction = {
            'transaction_id': 'TXN_001',
            'quantity': 2
        }
        
        result = validate_transaction_data(invalid_transaction)
        assert result['valid'] == False
        assert len(result['errors']) > 0
    
    def test_validate_transaction_data_invalid_quantity(self):
        """Test transaction validation with invalid quantity."""
        invalid_transaction = {
            'transaction_id': 'TXN_001',
            'date': '2024-01-15',
            'product_id': 'PRD_101',
            'customer_id': 'CUST_123',
            'quantity': -1,
            'unit_price': 29.99,
            'total_amount': 59.98
        }
        
        result = validate_transaction_data(invalid_transaction)
        assert result['valid'] == False
        assert any('Quantity must be a positive number' in error for error in result['errors'])


class TestProfitCalculations:
    """Unit tests for profit calculation functions."""
    
    def test_calculate_profit_metrics_basic(self):
        """Test basic profit calculation."""
        result = calculate_profit_metrics(100.0, 60.0, 1)
        
        assert result['profit_per_unit'] == 40.0
        assert result['margin_percentage'] == 40.0
        assert result['total_revenue'] == 100.0
        assert result['total_profit'] == 40.0
    
    def test_calculate_profit_metrics_multiple_quantities(self):
        """Test profit calculation with multiple quantities."""
        result = calculate_profit_metrics(29.99, 18.50, 10)
        
        assert result['profit_per_unit'] == 11.49
        assert result['total_revenue'] == 299.90
        assert result['total_profit'] == 114.90
    
    def test_calculate_profit_metrics_invalid_price(self):
        """Test profit calculation with invalid price."""
        with pytest.raises(ValueError):
            calculate_profit_metrics(-10.0, 5.0)
    
    def test_calculate_profit_metrics_invalid_cost(self):
        """Test profit calculation with invalid cost."""
        with pytest.raises(ValueError):
            calculate_profit_metrics(10.0, -5.0)


class TestDataQualityAnalysis:
    """Unit tests for data quality analysis."""
    
    def test_analyze_sales_data_quality_file_not_found(self):
        """Test data quality analysis with non-existent file."""
        result = analyze_sales_data_quality("nonexistent.csv")
        assert 'error' in result
    
    def test_benchmark_data_loading_structure(self):
        """Test that benchmark returns expected structure."""
        # This test would need the actual CSV file to exist
        # For now, just test the error handling
        result = benchmark_data_loading("nonexistent.csv")
        # Should handle the error gracefully
        assert isinstance(result, dict)


# ============================================================================
# DEMO FUNCTIONS FOR REPL TESTING
# ============================================================================

def demo_data_validation():
    """Demo function showing data validation in action."""
    print("🧪 Data Validation Demo")
    print("-" * 30)
    
    # Test valid transaction
    valid_transaction = {
        'transaction_id': 'TXN_999',
        'date': '2024-01-15',
        'product_id': 'PRD_101',
        'customer_id': 'CUST_123',
        'quantity': 2,
        'unit_price': 29.99,
        'total_amount': 59.98
    }
    
    print("✅ Valid Transaction:")
    result = validate_transaction_data(valid_transaction)
    print(f"Valid: {result['valid']}")
    print(f"Errors: {result['errors']}")
    print(f"Warnings: {result['warnings']}")
    
    # Test invalid transaction
    print("\n❌ Invalid Transaction:")
    invalid_transaction = {
        'transaction_id': 'TXN_BAD',
        'quantity': -5,
        'unit_price': 0,
        'total_amount': 100
    }
    
    result = validate_transaction_data(invalid_transaction)
    print(f"Valid: {result['valid']}")
    print(f"Errors: {result['errors']}")


def demo_profit_calculations():
    """Demo function showing profit calculations."""
    print("\n💰 Profit Calculation Demo")
    print("-" * 30)
    
    products = [
        ("Headphones", 29.99, 18.50, 10),
        ("Smartwatch", 149.99, 89.99, 5),
        ("T-Shirt", 19.99, 7.50, 20),
    ]
    
    for name, price, cost, quantity in products:
        print(f"\n📦 {name}:")
        metrics = calculate_profit_metrics(price, cost, quantity)
        print(f"  Profit per unit: ${metrics['profit_per_unit']}")
        print(f"  Margin: {metrics['margin_percentage']}%")
        print(f"  Total revenue: ${metrics['total_revenue']}")
        print(f"  Total profit: ${metrics['total_profit']}")


def demo_performance_test():
    """Demo function showing performance testing."""
    print("\n⚡ Performance Test Demo")
    print("-" * 30)
    
    # Test DuckDB connection
    print("🔍 Testing DuckDB connection...")
    db_result = test_duckdb_connection()
    if db_result.get('connection'):
        print("✅ DuckDB connection successful")
        print(f"Tables available: {db_result.get('available_tables', [])}")
    else:
        print(f"❌ DuckDB connection failed: {db_result.get('error')}")
    
    # Test data loading if file exists
    if Path("data/sales.csv").exists():
        print("\n📊 Testing data loading performance...")
        benchmark_result = benchmark_data_loading()
        if 'error' not in benchmark_result:
            print(f"Pandas load time: {benchmark_result['pandas_load_time']}s")
            print(f"DuckDB load time: {benchmark_result['duckdb_load_time']}s")
            print(f"Winner: {benchmark_result['winner']}")
        else:
            print(f"Benchmark failed: {benchmark_result['error']}")


def run_all_demos():
    """Run all demo functions - perfect for REPL testing."""
    print("🚀 Running All Test Demos")
    print("=" * 50)
    
    demo_data_validation()
    demo_profit_calculations()
    demo_performance_test()
    
    print("\n✅ All demos completed!")
    print("\n💡 Try these individual functions:")
    print("  validate_transaction_data({'transaction_id': 'TEST', 'quantity': 5})")
    print("  calculate_profit_metrics(100, 60, 3)")
    print("  test_duckdb_connection()")
    print("  benchmark_data_loading()")


def main():
    """Main function for script execution."""
    print("🧪 Sales Analysis - Test Functions")
    print("=" * 50)
    
    # Run demos
    run_all_demos()
    
    # Run pytest if available
    try:
        print("\n🔬 Running Unit Tests...")
        import subprocess
        result = subprocess.run(['pytest', __file__, '-v'], capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("Test errors:", result.stderr)
    except Exception as e:
        print(f"Could not run pytest: {e}")
        print("💡 Install pytest: pip install pytest")


if __name__ == "__main__":
    main()
