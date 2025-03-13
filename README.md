# substrait-dataframe

A minimal reference implementation of creating a Python DataFrame library that can be adapted to execute on any engine that speaks [Substrait](https://substrait.io).
The actual DataFrame library functionality is _very_ limited and this repository is mainly to show the integration with Substrait.
i.e., this package is not intended to be used for analysis and is more for reviewing the code.

## Support Functionality

The functionality here is _very_ limited:

Substrait Features:

- Select: Selecting one or more fields
- Filter: Simple filtering on one or more string fields
- Limit

Backends:

- [x] DuckDB
- [x] DataFusion

# Missing Pieces

- Operations aren't composable. i.e., df.select(...).select(...) throws away all but the last .select(...). This applies to filter and limit as well.
- Ergonomics. The code is designed mainly to show the larger picture and a lot of quality of life improvements could be made to make the code more concise and user-friendly.

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

There's an example in the root of the repository at `./example.py` that shows the basic operation.
It's repeated below with output:

```python
>>> import duckdb
>>> from substrait_dataframe import DuckDBBackend, DataFrame, Expression, Field, Relation
>>>
>>> con = duckdb.connect()
>>>
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
...     backend=DuckDBBackend(con)
... )
...
>>>
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
0       Adelie  Torgersen
1       Adelie  Torgersen
2       Adelie  Torgersen
3       Adelie  Torgersen
4       Adelie  Torgersen
..         ...        ...
339  Chinstrap      Dream
340  Chinstrap      Dream
341  Chinstrap      Dream
342  Chinstrap      Dream
343  Chinstrap      Dream

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
0       Adelie
1       Adelie
2       Adelie
3       Adelie
4       Adelie
..         ...
339  Chinstrap
340  Chinstrap
341  Chinstrap
342  Chinstrap
343  Chinstrap

[344 rows x 1 columns]
>>> (
...     df.select([Field("island", "string"), Field("species", "string")])
...     .filter(Expression.IsInStringLiteral(Field("island", "string"), "Dream"))
...     .execute()
...     .to_pandas()
... )
...
        island species
0       Adelie   Dream
1       Adelie   Dream
2       Adelie   Dream
3       Adelie   Dream
4       Adelie   Dream
..         ...     ...
119  Chinstrap   Dream
120  Chinstrap   Dream
121  Chinstrap   Dream
122  Chinstrap   Dream
123  Chinstrap   Dream

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
0  Adelie   Dream
1  Adelie   Dream
2  Adelie   Dream
3  Adelie   Dream
4  Adelie   Dream
```
