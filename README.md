# df-enrich

[![CI](https://github.com/elphick/df-enrich/actions/workflows/ci.yml/badge.svg)](https://github.com/elphick/df-enrich/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

Pandas DataFrame accessor for schema-driven validation, derived columns, lookups, and profiling—powered by df-eval.

## Features

- **🔍 Validation**: Schema-driven validation using [Pandera](https://pandera.readthedocs.io/)
- **🔧 Derivation**: Create derived columns using expressions (via df-eval Engine)
- **📊 Profiling**: Generate data profiles with [ydata-profiling](https://github.com/ydataai/ydata-profiling)
- **🔗 Lookups**: Enrich data with external sources
- **⛓️ Chaining**: Fluent API for method chaining
- **📝 Provenance**: Track operations in DataFrame.attrs
- **🎯 Type Casting**: Convenient dtype conversions

## Installation

Using uv (recommended):
```bash
uv pip install df-enrich
```

Using pip:
```bash
pip install df-enrich
```

With optional dependencies:
```bash
# For documentation
uv pip install "df-enrich[docs]"

# For testing
uv pip install "df-enrich[test]"
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

## Usage

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

# From YAML string
df.enrich.derive("""
total: "price * quantity"
discount: "total * 0.1"
""")

# From YAML file
df.enrich.derive("derivations.yaml")
```

### Profiling

```python
# Generate profile report
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

### Configuration

```python
# Configure registries, secrets, etc.
df.enrich.config(registry_url="https://api.example.com")
```

## Development

### Setup

```bash
# Clone repository
git clone https://github.com/elphick/df-enrich.git
cd df-enrich

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
uv pip install -e ".[test,docs]"
```

### Running Tests

```bash
uv run pytest tests/ -v --cov=df_enrich
```

### Building Documentation

```bash
cd docs
uv run sphinx-build -b html . _build/html
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
