# df-enrich

Pandas DataFrame accessor for schema-driven validation, derived columns, lookups, and profiling—powered by df-eval.

## Installation

```bash
pip install df-enrich
```

## Quick Start

```python
import pandas as pd
import pandera as pa
from df_enrich import EnrichAccessor

# Create a DataFrame
df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

# Validate with Pandera
schema = pa.DataFrameSchema({
    "a": pa.Column(int),
    "b": pa.Column(int)
})
df_validated = df.enrich.validate(schema)

# Derive new columns
df_enriched = df.enrich.derive({"c": "a + b", "d": "a * b"})

# Chain operations
result = (df.enrich
          .validate(schema)
          .enrich.derive({"total": "a + b"})
          .enrich.cast({"total": "int32"}))
```

## Features

- **Validation**: Schema-driven validation using Pandera
- **Derivation**: Create derived columns using expressions (via df-eval Engine)
- **Profiling**: Generate data profiles with ydata-profiling
- **Lookups**: Enrich data with external sources
- **Chaining**: Fluent API for method chaining
- **Provenance**: Track operations in DataFrame.attrs
- **Type Casting**: Convenient dtype conversions

## Contents

```{toctree}
:maxdepth: 2

api
examples
```

## API Reference

```{eval-rst}
.. automodule:: df_enrich.accessor
   :members:
   :undoc-members:
   :show-inheritance:
```

## Examples

### Validation

```python
import pandera as pa

class MySchema(pa.DataFrameModel):
    col1: int
    col2: float

df.enrich.validate(MySchema)
```

### Derive Columns

```python
# From dictionary
df.enrich.derive({"total": "price * quantity"})

# From YAML
df.enrich.derive("""
total: "price * quantity"
discount: "total * 0.1"
""")
```

### Profiling

```python
# Generate full profile
profile = df.enrich.profile(engine="ydata")

# Lazy profiling
profile = df.enrich.profile(engine="ydata", lazy=True)
```

### Lookups

```python
# Lookup from DataFrame
prices = pd.DataFrame({"price": [10, 20, 30]})
df.enrich.lookup(prices, dst="price")

# Custom resolver
def my_resolver(df, src, dst):
    df[dst] = fetch_from_api(df)
    return df

df.enrich.lookup(None, dst="data", resolver=my_resolver)
```

## License

MIT
