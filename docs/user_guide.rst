User Guide
==========

Core concepts
-------------

``df-enrich`` extends ``pandas.DataFrame`` with an ``enrich`` accessor that
supports validation, derivation, lookups, profiling, casting, and
configuration.

Validation
----------

Use Pandera schemas to validate input data before downstream operations.

.. code-block:: python

   import pandera as pa

   schema = pa.DataFrameSchema({
       "product": pa.Column(str),
       "price": pa.Column(float),
       "quantity": pa.Column(int),
   })

   validated = df.enrich.validate(schema)

Derivations
-----------

Create derived columns from expressions.

.. code-block:: python

   enriched = df.enrich.derive({
       "total": "price * quantity",
       "discount": "total * 0.1",
       "final_price": "total - discount",
   })

You can also provide YAML content or a YAML file path.

.. code-block:: python

   df.enrich.derive("""
   total: "price * quantity"
   discount: "total * 0.1"
   """)

Lookups
-------

Enrich data from another DataFrame or custom resolver.

.. code-block:: python

   categories = pd.DataFrame({"category": ["Electronics", "Clothing", "Food"]}, index=["A", "B", "C"])
   with_category = df.set_index("product").enrich.lookup(categories, dst="category")

Profiling
---------

Generate data quality summaries.

.. code-block:: python

   profile = df.enrich.profile()

Chaining operations
-------------------

Methods are designed for fluent chaining.

.. code-block:: python

   result = (
       df.enrich
       .validate(schema)
       .enrich.derive({"total": "price * quantity"})
       .enrich.cast({"total": "float32"})
   )

See also
--------

* :doc:`auto_examples/index`
* :doc:`api/modules`
