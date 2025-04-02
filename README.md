# substrait-dataframe

A barebones reference implementation Python DataFrame library that speaks [Substrait](https://substrait.io).

This is not intended to be used for any real work or to be built on top of. I am sharing it in case it helps anyone who might be evaluating [Substrait](https://substrait.io) get any idea for what building on top of it might look like.

## What This Can Do

The functionality here is barebones:

Supported Backends:

- [x] DuckDB
- [x] DataFusion

Supported DataFrame Operations:

- [x] Select: Selecting one or more fields
- [x] Filter: Simple filtering on one or more string fields
- [x] Limit

## What This Can't Do

This list isn't comprehensive but here are some things you might notice this can't do:

- Operations aren't composable. i.e., `df.select(...).select(...)` throws away all but the last `.select(...)`. This applies to filter and limit as well.
- No ergonomics. The code is designed mainly to show the larger picture and a lot of quality of life improvements could be made to make the code more concise and user-friendly.
- Break outside of Python. This would be much better if it could send a query to an external database using [ADBC](https://arrow.apache.org/adbc/current/index.html) for example.

## Related Work

- [tokoko/subframe](https://github.com/tokoko/subframe): A much more advanced and feature-full version of what I've done here. DataFrame API that speaks [Substrait](https://substrait.io) over [ADBC](https://arrow.apache.org/adbc/current/index.html). I highly recommend taking a look at this.
- [sqlframe](https://github.com/eakmanrq/sqlframe): Provides a PySpark-like DataFrame API that can speak to Spark and non-Spark databases by translating PySpark to SQL.

## Installing

Assuming a standard Python installation with access to pip:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install .[all] # To enable DuckDB and DataFusion
```

## Testing

Using the above venv, run:

```sh
python -m pip install .[testing]
pytest
```

## Example

There's an example in the root of the repository at `./example.py` that shows the basic operation with the DuckDB and Datafusion backends.
It's repeated below with output:

```python
>>> import duckdb
>>> import datafusion
>>> import pyarrow as pa
>>> from substrait_dataframe import (
...     DatafusionBackend,
...     DuckDBBackend,
...     DataFrame,
...     Expression,
...     Field,
...     Relation,
... )
...
>>> con = duckdb.connect()
>>> con.sql("INSTALL substrait FROM community;")
>>> con.sql("LOAD substrait;")
>>> con.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")
>>> df = DataFrame(
...     relation=Relation(
...         name="penguins",
...         fields=[
...             Field("species", "string"),
...             Field("island", "string"),
...             Field("bill_length_mm", "fp64"),
...             Field("bill_depth_mm", "fp64"),
...             Field("flipper_length_mm", "i32"),
...             Field("body_mass_g", "i32"),
...             Field("sex", "string"),
...             Field("year", "i32"),
...         ],
...     ),
...     backend=DuckDBBackend(con),
... )
...
>>> df.execute().to_pandas()
       species     island  bill_length_mm  bill_depth_mm  flipper_length_mm  body_mass_g     sex  year
0       Adelie  Torgersen            39.1           18.7              181.0       3750.0    male  2007
1       Adelie  Torgersen            39.5           17.4              186.0       3800.0  female  2007
2       Adelie  Torgersen            40.3           18.0              195.0       3250.0  female  2007
3       Adelie  Torgersen             NaN            NaN                NaN          NaN    None  2007
4       Adelie  Torgersen            36.7           19.3              193.0       3450.0  female  2007
..         ...        ...             ...            ...                ...          ...     ...   ...
339  Chinstrap      Dream            55.8           19.8              207.0       4000.0    male  2009
340  Chinstrap      Dream            43.5           18.1              202.0       3400.0  female  2009
341  Chinstrap      Dream            49.6           18.2              193.0       3775.0    male  2009
342  Chinstrap      Dream            50.8           19.0              210.0       4100.0    male  2009
343  Chinstrap      Dream            50.2           18.7              198.0       3775.0  female  2009

[344 rows x 8 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .execute()
...     .to_pandas()
... )
...
        island    species
0    Torgersen     Adelie
1    Torgersen     Adelie
2    Torgersen     Adelie
3    Torgersen     Adelie
4    Torgersen     Adelie
..         ...        ...
339      Dream  Chinstrap
340      Dream  Chinstrap
341      Dream  Chinstrap
342      Dream  Chinstrap
343      Dream  Chinstrap

[344 rows x 2 columns]
>>> (
...     df.select(
...         [
...             Field("island", "string"),
...         ]
...     )
...     .execute()
...     .to_pandas()
... )
...
        island
0    Torgersen
1    Torgersen
2    Torgersen
3    Torgersen
4    Torgersen
..         ...
339      Dream
340      Dream
341      Dream
342      Dream
343      Dream

[344 rows x 1 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
...     .execute()
...     .to_pandas()
... )
...
    island    species
0    Dream     Adelie
1    Dream     Adelie
2    Dream     Adelie
3    Dream     Adelie
4    Dream     Adelie
..     ...        ...
119  Dream  Chinstrap
120  Dream  Chinstrap
121  Dream  Chinstrap
122  Dream  Chinstrap
123  Dream  Chinstrap

[124 rows x 2 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
...     .limit(5)
...     .execute()
...     .to_pandas()
... )
...
  island species
0  Dream  Adelie
1  Dream  Adelie
2  Dream  Adelie
3  Dream  Adelie
4  Dream  Adelie
>>> ctx = datafusion.SessionContext(datafusion.SessionConfig())
>>> schema = pa.schema(
...     [
...         pa.field("species", pa.string()),
...         pa.field("island", pa.string()),
...         pa.field("bill_length_mm", pa.float64()),
...         pa.field("bill_depth_mm", pa.float64()),
...         pa.field("flipper_length_mm", pa.int32()),
...         pa.field("body_mass_g", pa.int32()),
...         pa.field("sex", pa.string()),
...         pa.field("year", pa.int32()),
...     ]
... )
...
>>> ctx.register_parquet("penguins", "./data/penguins.parquet", schema=schema)
>>> df = DataFrame(
...     relation=Relation(
...         name="penguins",
...         fields=[
...             Field("species", "string"),
...             Field("island", "string"),
...             Field("bill_length_mm", "fp64"),
...             Field("bill_depth_mm", "fp64"),
...             Field("flipper_length_mm", "i32"),
...             Field("body_mass_g", "i32"),
...             Field("sex", "string"),
...             Field("year", "i32"),
...         ],
...     ),
...     backend=DatafusionBackend(ctx),  # <-- Only this changes from before
... )
...
>>> df.execute().to_pandas()
       species     island  bill_length_mm  bill_depth_mm  flipper_length_mm  body_mass_g     sex  year
0       Adelie  Torgersen            39.1           18.7              181.0       3750.0    male  2007
1       Adelie  Torgersen            39.5           17.4              186.0       3800.0  female  2007
2       Adelie  Torgersen            40.3           18.0              195.0       3250.0  female  2007
3       Adelie  Torgersen             NaN            NaN                NaN          NaN    None  2007
4       Adelie  Torgersen            36.7           19.3              193.0       3450.0  female  2007
..         ...        ...             ...            ...                ...          ...     ...   ...
339  Chinstrap      Dream            55.8           19.8              207.0       4000.0    male  2009
340  Chinstrap      Dream            43.5           18.1              202.0       3400.0  female  2009
341  Chinstrap      Dream            49.6           18.2              193.0       3775.0    male  2009
342  Chinstrap      Dream            50.8           19.0              210.0       4100.0    male  2009
343  Chinstrap      Dream            50.2           18.7              198.0       3775.0  female  2009

[344 rows x 8 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .execute()
...     .to_pandas()
... )
...
        island    species
0    Torgersen     Adelie
1    Torgersen     Adelie
2    Torgersen     Adelie
3    Torgersen     Adelie
4    Torgersen     Adelie
..         ...        ...
339      Dream  Chinstrap
340      Dream  Chinstrap
341      Dream  Chinstrap
342      Dream  Chinstrap
343      Dream  Chinstrap

[344 rows x 2 columns]
>>> (
...     df.select(
...         [
...             Field("island", "string"),
...         ]
...     )
...     .execute()
...     .to_pandas()
... )
...
        island
0    Torgersen
1    Torgersen
2    Torgersen
3    Torgersen
4    Torgersen
..         ...
339      Dream
340      Dream
341      Dream
342      Dream
343      Dream

[344 rows x 1 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
...     .execute()
...     .to_pandas()
... )
...
    island    species
0    Dream     Adelie
1    Dream     Adelie
2    Dream     Adelie
3    Dream     Adelie
4    Dream     Adelie
..     ...        ...
119  Dream  Chinstrap
120  Dream  Chinstrap
121  Dream  Chinstrap
122  Dream  Chinstrap
123  Dream  Chinstrap

[124 rows x 2 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
...     .limit(5)
...     .execute()
...     .to_pandas()
... )
...
  island species
0  Dream  Adelie
1  Dream  Adelie
2  Dream  Adelie
3  Dream  Adelie
4  Dream  Adelie
```
