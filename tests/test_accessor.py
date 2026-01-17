"""
Tests for df-enrich accessor.
"""
import pytest
import pandas as pd
import pandera as pa
from df_enrich import EnrichAccessor


class TestEnrichAccessor:
    """Test EnrichAccessor registration and basic functionality."""
    
    def test_accessor_registration(self):
        """Test that the accessor is properly registered."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert hasattr(df, "enrich")
        assert isinstance(df.enrich, EnrichAccessor)
    
    def test_accessor_object_reference(self):
        """Test that accessor maintains reference to parent DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert df.enrich._obj is df


class TestValidate:
    """Test validate() method."""
    
    def test_validate_with_schema(self):
        """Test validation with a Pandera schema."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        schema = pa.DataFrameSchema({
            "a": pa.Column(int),
            "b": pa.Column(int)
        })
        
        result = df.enrich.validate(schema)
        assert isinstance(result, pd.DataFrame)
        assert "enrich_validated" in result.attrs
        assert result.attrs["enrich_validated"] is True
    
    def test_validate_with_schema_model(self):
        """Test validation with a Pandera SchemaModel."""
        class TestSchema(pa.DataFrameModel):
            a: int
            b: int
        
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = df.enrich.validate(TestSchema)
        
        assert isinstance(result, pd.DataFrame)
        assert "enrich_validated" in result.attrs
    
    def test_validate_failure(self):
        """Test that validation raises on schema violation."""
        df = pd.DataFrame({"a": ["x", "y", "z"], "b": [4, 5, 6]})
        schema = pa.DataFrameSchema({
            "a": pa.Column(int),
            "b": pa.Column(int)
        })
        
        with pytest.raises(pa.errors.SchemaError):
            df.enrich.validate(schema)


class TestDerive:
    """Test derive() method."""
    
    def test_derive_with_dict(self):
        """Test deriving columns with a dictionary spec."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = df.enrich.derive({"c": "a + b"})
        
        assert "c" in result.columns
        assert list(result["c"]) == [5, 7, 9]
        assert "enrich_derived" in result.attrs
        assert result.attrs["enrich_derived"] is True
    
    def test_derive_multiple_columns(self):
        """Test deriving multiple columns."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = df.enrich.derive({
            "c": "a + b",
            "d": "a * b"
        })
        
        assert "c" in result.columns
        assert "d" in result.columns
        assert list(result["c"]) == [5, 7, 9]
        assert list(result["d"]) == [4, 10, 18]
    
    def test_derive_with_yaml_string(self):
        """Test deriving columns with YAML string."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        yaml_spec = """
        c: "a + b"
        d: "a * 2"
        """
        result = df.enrich.derive(yaml_spec)
        
        assert "c" in result.columns
        assert "d" in result.columns
    
    def test_derive_invalid_expression(self):
        """Test that invalid expressions raise errors."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        with pytest.raises(ValueError):
            df.enrich.derive({"bad": "nonexistent_col + 1"})


class TestProfile:
    """Test profile() method."""
    
    def test_profile_basic_fallback(self):
        """Test basic profiling without ydata-profiling."""
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [5, 4, 3, 2, 1]})
        
        # This will use the fallback if ydata-profiling is not installed
        try:
            profile = df.enrich.profile()
            # If ydata-profiling is installed, profile will be ProfileReport
            # Otherwise, it will be a dict
            assert profile is not None
        except ImportError:
            pytest.skip("ydata-profiling not installed")
    
    def test_profile_unsupported_engine(self):
        """Test that unsupported engines raise errors."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        with pytest.raises(ValueError, match="Unsupported profiling engine"):
            df.enrich.profile(engine="unsupported")


class TestLookup:
    """Test lookup() method."""
    
    def test_lookup_with_dataframe(self):
        """Test lookup with a DataFrame source."""
        df = pd.DataFrame({"a": [1, 2, 3]}, index=[0, 1, 2])
        lookup_df = pd.DataFrame({"price": [10, 20, 30]}, index=[0, 1, 2])
        
        result = df.enrich.lookup(lookup_df, dst="price")
        
        assert "price" in result.columns
        assert "enrich_lookup" in result.attrs
    
    def test_lookup_with_missing_columns(self):
        """Test that lookup validates column existence in source."""
        df = pd.DataFrame({"a": [1, 2, 3]}, index=[0, 1, 2])
        lookup_df = pd.DataFrame({"price": [10, 20, 30]}, index=[0, 1, 2])
        
        with pytest.raises(ValueError, match="not found in source DataFrame"):
            df.enrich.lookup(lookup_df, dst="nonexistent_col")
    
    def test_lookup_with_string_source(self):
        """Test that string sources raise NotImplementedError."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        with pytest.raises(NotImplementedError):
            df.enrich.lookup("registry://data", dst="col")
    
    def test_lookup_with_resolver(self):
        """Test lookup with custom resolver function."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        def custom_resolver(df, src, dst):
            df[dst] = df["a"] * 10
            return df
        
        result = df.enrich.lookup(None, dst="b", resolver=custom_resolver)
        
        assert "b" in result.columns
        assert list(result["b"]) == [10, 20, 30]


class TestCast:
    """Test cast() method."""
    
    def test_cast_dtypes(self):
        """Test casting column dtypes."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        result = df.enrich.cast({"a": "int64", "b": "float32"})
        
        assert result["a"].dtype == "int64"
        assert result["b"].dtype == "float32"
        assert "enrich_cast" in result.attrs
    
    def test_cast_with_missing_columns(self):
        """Test that cast warns about missing columns."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        with pytest.warns(UserWarning, match="not found in DataFrame"):
            result = df.enrich.cast({"a": "int64", "nonexistent": "float32"})
        
        # Should still cast existing columns
        assert result["a"].dtype == "int64"


class TestConfig:
    """Test config() method."""
    
    def test_config_setting(self):
        """Test setting configuration options."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        accessor = df.enrich.config(registry_url="https://api.example.com")
        
        assert accessor._config["registry_url"] == "https://api.example.com"
        assert isinstance(accessor, EnrichAccessor)


class TestChaining:
    """Test method chaining capabilities."""
    
    def test_method_chaining(self):
        """Test that methods can be chained together."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        schema = pa.DataFrameSchema({
            "a": pa.Column(int),
            "b": pa.Column(int)
        })
        
        result = (df.enrich
                  .validate(schema)
                  .enrich.derive({"c": "a + b"})
                  .enrich.cast({"c": "int32"}))
        
        assert "c" in result.columns
        assert result["c"].dtype == "int32"
        assert "enrich_validated" in result.attrs
        assert "enrich_derived" in result.attrs
        assert "enrich_cast" in result.attrs


class TestProvenance:
    """Test provenance tracking in DataFrame attrs."""
    
    def test_provenance_tracking(self):
        """Test that operations track provenance in df.attrs."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        schema = pa.DataFrameSchema({
            "a": pa.Column(int),
            "b": pa.Column(int)
        })
        
        result = df.enrich.validate(schema)
        
        assert hasattr(result, "attrs")
        assert "enrich_validated" in result.attrs
        assert "enrich_schema" in result.attrs
    
    def test_provenance_derive(self):
        """Test provenance for derive operations."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = df.enrich.derive({"b": "a * 2"})
        
        assert "enrich_derived" in result.attrs
        assert "enrich_derivations" in result.attrs
        assert result.attrs["enrich_derivations"] == {"b": "a * 2"}
