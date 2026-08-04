"""
Basic usage
===========

This example shows validation, derivation, lookup, profiling, and chaining with
the ``df.enrich`` accessor.
"""

import pandas as pd
import pandera.pandas as pa
from df_enrich import EnrichAccessor

# %%
# Create sample data.
df = pd.DataFrame(
    {
        "product": ["A", "B", "C"],
        "price": [10.0, 20.0, 30.0],
        "quantity": [2, 3, 1],
    }
)
df

# %%
# Validate against a Pandera schema.
schema = pa.DataFrameSchema(
    {
        "product": pa.Column(str),
        "price": pa.Column(float),
        "quantity": pa.Column(int),
    }
)
df_validated = df.enrich.validate(schema)
df_validated

# %%
# Derive new columns from expressions.
df_enriched = df.enrich.derive(
    {
        "total": "price * quantity",
        "discount": "total * 0.1",
        "final_price": "total - discount",
    }
)
df_enriched

# %%
# Chain multiple enrich operations.
result = (
    df.enrich
    .validate(schema)
    .enrich.derive({"total": "price * quantity"})
    .enrich.cast({"total": "float32"})
    .enrich.derive({"tax": "total * 0.08"})
)
result

# %%
# Lookup additional attributes from another DataFrame.
categories = pd.DataFrame({"category": ["Electronics", "Clothing", "Food"]}, index=["A", "B", "C"])
df_with_category = df.set_index("product").enrich.lookup(categories, dst="category")
df_with_category

# %%
# Generate a profile.
profile = df.enrich.profile()
profile
