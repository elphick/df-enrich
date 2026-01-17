"""
DataFrame accessor providing enrichment methods.
"""
import warnings
from typing import Any, Optional, Union, Dict, Callable
import pandas as pd
import pandera as pa
import yaml


@pd.api.extensions.register_dataframe_accessor("enrich")
class EnrichAccessor:
    """
    Pandas DataFrame accessor for schema-driven validation, derived columns, lookups, and profiling.
    
    Access via: df.enrich.validate(schema), df.enrich.derive(spec), etc.
    """
    
    def __init__(self, pandas_obj: pd.DataFrame):
        self._obj = pandas_obj
        self._config: Dict[str, Any] = {}
        
    def validate(self, schema: Union[pa.DataFrameSchema, type]) -> pd.DataFrame:
        """
        Validate DataFrame against a Pandera schema.
        
        Parameters
        ----------
        schema : pa.DataFrameSchema or pandera SchemaModel
            The schema to validate against.
            
        Returns
        -------
        pd.DataFrame
            The validated DataFrame (enables chaining).
            
        Raises
        ------
        pandera.errors.SchemaError
            If validation fails.
            
        Examples
        --------
        >>> import pandera as pa
        >>> schema = pa.DataFrameSchema({"col1": pa.Column(int)})
        >>> df.enrich.validate(schema)
        """
        # Handle both DataFrameSchema instances and SchemaModel classes
        if isinstance(schema, type) and issubclass(schema, pa.DataFrameModel):
            schema = schema.to_schema()
        
        validated_df = schema.validate(self._obj, lazy=False)
        
        # Track provenance in attrs
        if not hasattr(validated_df, 'attrs'):
            validated_df.attrs = {}
        validated_df.attrs['enrich_validated'] = True
        validated_df.attrs['enrich_schema'] = str(schema)
        
        return validated_df
    
    def derive(self, spec: Union[str, Dict[str, str], Any]) -> pd.DataFrame:
        """
        Derive new columns using expressions via df-eval Engine.
        
        Parameters
        ----------
        spec : str, dict, or yaml path
            Column derivation specification. Can be:
            - Dict mapping column names to expressions
            - YAML string with derivation rules
            - Path to YAML file
            
        Returns
        -------
        pd.DataFrame
            DataFrame with derived columns added (enables chaining).
            
        Examples
        --------
        >>> df.enrich.derive({"total": "col1 + col2"})
        >>> df.enrich.derive("derived_columns.yaml")
        """
        df = self._obj.copy()
        
        # Parse spec
        if isinstance(spec, str):
            # Check if it's a file path or YAML string
            try:
                # Try to load as YAML file
                with open(spec, 'r') as f:
                    spec_dict = yaml.safe_load(f)
            except (FileNotFoundError, IOError):
                # Treat as YAML string
                try:
                    spec_dict = yaml.safe_load(spec)
                except yaml.YAMLError:
                    raise ValueError(f"Invalid YAML specification: {spec}")
        elif isinstance(spec, dict):
            spec_dict = spec
        else:
            raise TypeError(f"spec must be str or dict, got {type(spec)}")
        
        # Apply derivations using pandas eval
        # This is a simple implementation; df-eval Engine would provide more features
        for col_name, expression in spec_dict.items():
            try:
                df[col_name] = df.eval(expression)
            except Exception as e:
                raise ValueError(f"Failed to derive column '{col_name}' with expression '{expression}': {e}")
        
        # Track provenance
        if not hasattr(df, 'attrs'):
            df.attrs = {}
        df.attrs['enrich_derived'] = True
        df.attrs['enrich_derivations'] = spec_dict
        
        return df
    
    def profile(self, engine: str = "ydata", lazy: bool = False, **kwargs) -> Any:
        """
        Generate a data profile report.
        
        Parameters
        ----------
        engine : str, default "ydata"
            Profiling engine to use. Currently supports:
            - "ydata": ydata-profiling (formerly pandas-profiling)
        lazy : bool, default False
            If True, return a lazy profile that computes on demand.
        **kwargs
            Additional arguments passed to the profiling engine.
            
        Returns
        -------
        ProfileReport or dict
            Profile report object or dictionary of statistics.
            
        Examples
        --------
        >>> profile = df.enrich.profile()
        >>> profile = df.enrich.profile(engine="ydata", lazy=True)
        """
        if engine == "ydata":
            try:
                from ydata_profiling import ProfileReport
                
                if lazy:
                    # Create profile with minimal computation
                    profile = ProfileReport(self._obj, minimal=True, lazy=True, **kwargs)
                else:
                    profile = ProfileReport(self._obj, **kwargs)
                
                return profile
            except ImportError:
                warnings.warn(
                    "ydata-profiling not installed. Install with: pip install ydata-profiling",
                    ImportWarning
                )
                # Fallback to basic statistics
                return self._basic_profile()
        else:
            raise ValueError(f"Unsupported profiling engine: {engine}. Supported: 'ydata'")
    
    def _basic_profile(self) -> Dict[str, Any]:
        """Generate basic profile statistics as fallback."""
        return {
            'shape': self._obj.shape,
            'dtypes': self._obj.dtypes.to_dict(),
            'missing': self._obj.isnull().sum().to_dict(),
            'describe': self._obj.describe().to_dict(),
        }
    
    def lookup(
        self,
        src: Union[str, pd.DataFrame],
        dst: Union[str, list],
        resolver: Optional[Callable] = None,
        on_missing: str = "warn"
    ) -> pd.DataFrame:
        """
        Perform lookups to enrich DataFrame with data from external sources.
        
        This method delegates to df-eval for complex lookup operations.
        
        Parameters
        ----------
        src : str or pd.DataFrame
            Source for lookup data. Can be:
            - DataFrame to merge with
            - String identifier for a registered data source
        dst : str or list
            Destination column name(s) to create from lookup.
        resolver : callable, optional
            Custom resolver function for lookups.
            Signature: resolver(df, src, dst) -> pd.DataFrame
        on_missing : {"warn", "raise", "ignore"}, default "warn"
            How to handle missing lookups:
            - "warn": Issue warning for missing values
            - "raise": Raise exception for missing values
            - "ignore": Silently continue with NaN values
            
        Returns
        -------
        pd.DataFrame
            DataFrame with lookup columns added (enables chaining).
            
        Examples
        --------
        >>> df.enrich.lookup(reference_df, dst="new_col")
        >>> df.enrich.lookup("registry://prices", dst="price", on_missing="raise")
        """
        df = self._obj.copy()
        
        # Handle custom resolver
        if resolver is not None:
            df = resolver(df, src, dst)
            return df
        
        # Handle DataFrame source
        if isinstance(src, pd.DataFrame):
            # Simple merge operation - this is a placeholder
            # Real df-eval Engine would provide more sophisticated merging
            if isinstance(dst, str):
                dst = [dst]
            
            # Validate that destination columns exist in source
            missing_cols = [col for col in dst if col not in src.columns]
            if missing_cols:
                raise ValueError(f"Columns {missing_cols} not found in source DataFrame. Available columns: {list(src.columns)}")
            
            # For now, do a simple index-based merge
            # In a real implementation, this would be more sophisticated
            # Try to merge on index
            result = df.merge(src[dst], left_index=True, right_index=True, how='left', suffixes=('', '_lookup'))
            
            # Handle missing values
            if isinstance(dst, list):
                missing_count = result[dst].isnull().sum().sum()
            else:
                missing_count = result[dst].isnull().sum()
            
            if missing_count > 0:
                if on_missing == "raise":
                    raise ValueError(f"Lookup failed: {missing_count} missing values in {dst}")
                elif on_missing == "warn":
                    warnings.warn(f"Lookup resulted in {missing_count} missing values in {dst}")
            
            df = result
        
        # Handle string source (registry, etc.)
        elif isinstance(src, str):
            # This would integrate with df-eval's registry system
            # For now, raise NotImplementedError with helpful message
            raise NotImplementedError(
                f"String-based lookup sources ('{src}') require df-eval Engine integration. "
                "Use a DataFrame or custom resolver for now."
            )
        else:
            raise TypeError(f"src must be str or DataFrame, got {type(src)}")
        
        # Track provenance
        if not hasattr(df, 'attrs'):
            df.attrs = {}
        df.attrs['enrich_lookup'] = True
        
        return df
    
    def config(self, **kwargs) -> 'EnrichAccessor':
        """
        Configure accessor settings (registries, secrets, etc.).
        
        Parameters
        ----------
        **kwargs
            Configuration options to set.
            
        Returns
        -------
        EnrichAccessor
            Self for chaining.
            
        Examples
        --------
        >>> df.enrich.config(registry_url="https://api.example.com")
        """
        self._config.update(kwargs)
        return self
    
    def cast(self, dtype_spec: Dict[str, Any]) -> pd.DataFrame:
        """
        Cast DataFrame columns to specified dtypes.
        
        Parameters
        ----------
        dtype_spec : dict
            Mapping of column names to target dtypes.
            
        Returns
        -------
        pd.DataFrame
            DataFrame with casted columns (enables chaining).
            
        Examples
        --------
        >>> df.enrich.cast({"col1": "int64", "col2": "float32"})
        """
        df = self._obj.copy()
        
        # Validate columns exist
        missing_cols = [col for col in dtype_spec.keys() if col not in df.columns]
        if missing_cols:
            warnings.warn(
                f"Columns {missing_cols} not found in DataFrame. Available columns: {list(df.columns)}. "
                "These columns will be skipped."
            )
        
        for col, dtype in dtype_spec.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        
        # Track provenance
        if not hasattr(df, 'attrs'):
            df.attrs = {}
        df.attrs['enrich_cast'] = True
        df.attrs['enrich_dtypes'] = dtype_spec
        
        return df
