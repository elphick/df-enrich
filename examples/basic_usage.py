#!/usr/bin/env python3
"""
Example usage of df-enrich package.
"""

import pandas as pd
import pandera as pa
from df_enrich import EnrichAccessor

def main():
    print("=== df-enrich Example ===\n")
    
    # Create sample data
    print("1. Creating sample DataFrame...")
    df = pd.DataFrame({
        "product": ["A", "B", "C"],
        "price": [10.0, 20.0, 30.0],
        "quantity": [2, 3, 1]
    })
    print(df)
    print()
    
    # Validate with Pandera
    print("2. Validating schema...")
    schema = pa.DataFrameSchema({
        "product": pa.Column(str),
        "price": pa.Column(float),
        "quantity": pa.Column(int)
    })
    
    df_validated = df.enrich.validate(schema)
    print(f"Schema validation passed! Attrs: {df_validated.attrs}")
    print()
    
    # Derive new columns
    print("3. Deriving new columns...")
    df_enriched = df.enrich.derive({
        "total": "price * quantity",
        "discount": "total * 0.1",
        "final_price": "total - discount"
    })
    print(df_enriched)
    print()
    
    # Chain operations
    print("4. Chaining operations...")
    result = (df.enrich
              .validate(schema)
              .enrich.derive({"total": "price * quantity"})
              .enrich.cast({"total": "float32"})
              .enrich.derive({"tax": "total * 0.08"}))
    
    print(result)
    print(f"\nResult dtypes:\n{result.dtypes}")
    print(f"\nResult attrs:\n{result.attrs}")
    print()
    
    # Lookup example
    print("5. Lookup example...")
    categories = pd.DataFrame({
        "category": ["Electronics", "Clothing", "Food"]
    }, index=["A", "B", "C"])
    
    df_indexed = df.set_index("product")
    df_with_category = df_indexed.enrich.lookup(categories, dst="category")
    print(df_with_category)
    print()
    
    # Profile example (basic fallback)
    print("6. Profiling example...")
    profile = df.enrich.profile()
    if isinstance(profile, dict):
        print("Profile (basic stats):")
        print(f"  Shape: {profile['shape']}")
        print(f"  Missing values: {profile['missing']}")
    else:
        print("Full profile report generated!")
    print()
    
    print("=== Example completed successfully! ===")

if __name__ == "__main__":
    main()
