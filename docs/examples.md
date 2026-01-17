# Examples

## Basic Usage

```python
import pandas as pd
import pandera as pa

# Create sample data
df = pd.DataFrame({
    "product": ["A", "B", "C"],
    "price": [10.0, 20.0, 30.0],
    "quantity": [2, 3, 1]
})

# Validate schema
schema = pa.DataFrameSchema({
    "product": pa.Column(str),
    "price": pa.Column(float),
    "quantity": pa.Column(int)
})

df_validated = df.enrich.validate(schema)
```

## Deriving Columns

```python
# Derive total cost
df_with_total = df.enrich.derive({"total": "price * quantity"})

# Multiple derivations
df_enriched = df.enrich.derive({
    "total": "price * quantity",
    "discount": "total * 0.1",
    "final_price": "total - discount"
})
```

## Chaining Operations

```python
result = (df.enrich
          .validate(schema)
          .enrich.derive({"total": "price * quantity"})
          .enrich.cast({"total": "float32"})
          .enrich.derive({"tax": "total * 0.08"}))
```

## Profiling

```python
# Generate profile report
profile = df.enrich.profile()

# Save to file
profile.to_file("report.html")
```

## Lookups

```python
# Lookup additional data
categories = pd.DataFrame({
    "category": ["Electronics", "Clothing", "Food"]
}, index=["A", "B", "C"])

df_with_category = df.set_index("product").enrich.lookup(
    categories, 
    dst="category"
)
```

## Provenance Tracking

```python
# Operations are tracked in attrs
result = df.enrich.validate(schema)
print(result.attrs)  # {'enrich_validated': True, ...}

result = result.enrich.derive({"total": "price * quantity"})
print(result.attrs)  # Includes derivation info
```
