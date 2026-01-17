# API Reference

## EnrichAccessor

```{eval-rst}
.. autoclass:: df_enrich.accessor.EnrichAccessor
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

## Methods

### validate

Validate DataFrame against a Pandera schema.

### derive

Derive new columns using expressions via df-eval Engine.

### profile

Generate a data profile report.

### lookup

Perform lookups to enrich DataFrame with data from external sources.

### cast

Cast DataFrame columns to specified dtypes.

### config

Configure accessor settings (registries, secrets, etc.).
