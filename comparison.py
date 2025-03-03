# How do two Substrait producers produce a plan for this relatively simple
# query?
#
#     SELECT * FROM penguins LIMIT 10;
#
# Let's see:

import duckdb
import json

print(duckdb.__version__)

con = duckdb.connect()

con.sql("INSTALL substrait FROM community")
con.sql("LOAD substrait")
con.sql("CREATE TABLE 'penguins' AS SELECT * FROM './data/penguins.parquet';")
# File above is available at https://files.brycemecum.com/penguins.parquet

query_str = "SELECT * FROM penguins LIMIT 10;"
con.sql(query_str)
# ┌─────────┬───────────┬────────────────┬───┬─────────────┬─────────┬───────┐
# │ species │  island   │ bill_length_mm │ … │ body_mass_g │   sex   │ year  │
# │ varchar │  varchar  │     double     │   │    int32    │ varchar │ int32 │
# ├─────────┼───────────┼────────────────┼───┼─────────────┼─────────┼───────┤
# │ Adelie  │ Torgersen │           39.1 │ … │        3750 │ male    │  2007 │
# │ Adelie  │ Torgersen │           39.5 │ … │        3800 │ female  │  2007 │
# │ Adelie  │ Torgersen │           40.3 │ … │        3250 │ female  │  2007 │
# │ Adelie  │ Torgersen │           NULL │ … │        NULL │ NULL    │  2007 │
# │ Adelie  │ Torgersen │           36.7 │ … │        3450 │ female  │  2007 │
# │ Adelie  │ Torgersen │           39.3 │ … │        3650 │ male    │  2007 │
# │ Adelie  │ Torgersen │           38.9 │ … │        3625 │ female  │  2007 │
# │ Adelie  │ Torgersen │           39.2 │ … │        4675 │ male    │  2007 │
# │ Adelie  │ Torgersen │           34.1 │ … │        3475 │ NULL    │  2007 │
# │ Adelie  │ Torgersen │           42.0 │ … │        4250 │ NULL    │  2007 │
# ├─────────┴───────────┴────────────────┴───┴─────────────┴─────────┴───────┤
# │ 10 rows                                              8 columns (6 shown) │
# └──────────────────────────────────────────────────────────────────────────┘

# Based upon my reading of the Subtsrait spec, the plan for `SELECT * FROM
# penguins LIMIT 10;` should structured like this:

# Root
# |-- Fetch(offset=0, limit=10)
#     |-- Read


# Up first, DuckDB
plan_json = con.sql(f"CALL get_substrait_json('{query_str}')").fetchall()[0][0]
print(json.dumps(json.loads(plan_json), indent=4))

# Simplified from the above, the result is structured like this:
#
# Root
# |-- Project
#     |-- Project
#         |-- Sort
#             |-- Project
#                 |-- Join
#                     |-- Left
#                     |   |-- Read
#                     |-- Right
#                         |-- Fetch(offset=0, limit=10)
#                             |-- Read
#

# Notes:
#
# - Uses projects, sorts, and joins for no reason?
# - Uses function (equal) for the join maybe?

# Does this query produce the correct result?
con.sql(f"CALL from_substrait_json('{plan_json}')")

# ┌─────────┬───────────┬────────────────┬───┬─────────────┬─────────┬───────┐
# │ species │  island   │ bill_length_mm │ … │ body_mass_g │   sex   │ year  │
# │ varchar │  varchar  │     double     │   │    int32    │ varchar │ int32 │
# ├─────────┼───────────┼────────────────┼───┼─────────────┼─────────┼───────┤
# │ Adelie  │ Torgersen │           39.1 │ … │        3750 │ male    │  2007 │
# │ Adelie  │ Torgersen │           39.5 │ … │        3800 │ female  │  2007 │
# │ Adelie  │ Torgersen │           40.3 │ … │        3250 │ female  │  2007 │
# │ Adelie  │ Torgersen │           NULL │ … │        NULL │ NULL    │  2007 │
# │ Adelie  │ Torgersen │           36.7 │ … │        3450 │ female  │  2007 │
# │ Adelie  │ Torgersen │           39.3 │ … │        3650 │ male    │  2007 │
# │ Adelie  │ Torgersen │           38.9 │ … │        3625 │ female  │  2007 │
# │ Adelie  │ Torgersen │           39.2 │ … │        4675 │ male    │  2007 │
# │ Adelie  │ Torgersen │           34.1 │ … │        3475 │ NULL    │  2007 │
# │ Adelie  │ Torgersen │           42.0 │ … │        4250 │ NULL    │  2007 │
# │   ·     │   ·       │             ·  │ · │          ·  │  ·      │    ·  │
# │   ·     │   ·       │             ·  │ · │          ·  │  ·      │    ·  │
# │   ·     │   ·       │             ·  │ · │          ·  │  ·      │    ·  │
# │ Adelie  │ Dream     │           32.1 │ … │        3050 │ female  │  2009 │
# │ Adelie  │ Dream     │           40.7 │ … │        3725 │ male    │  2009 │
# │ Adelie  │ Dream     │           37.3 │ … │        3000 │ female  │  2009 │
# │ Adelie  │ Dream     │           39.0 │ … │        3650 │ male    │  2009 │
# │ Adelie  │ Dream     │           39.2 │ … │        4250 │ male    │  2009 │
# │ Adelie  │ Dream     │           36.6 │ … │        3475 │ female  │  2009 │
# │ Adelie  │ Dream     │           36.0 │ … │        3450 │ female  │  2009 │
# │ Adelie  │ Dream     │           37.8 │ … │        3750 │ male    │  2009 │
# │ Adelie  │ Dream     │           36.0 │ … │        3700 │ female  │  2009 │
# │ Adelie  │ Dream     │           41.5 │ … │        4000 │ male    │  2009 │
# ├─────────┴───────────┴────────────────┴───┴─────────────┴─────────┴───────┤
# │ 152 rows (20 shown)                                  8 columns (6 shown) │
# └──────────────────────────────────────────────────────────────────────────┘

# This looks like a bug in the DuckDB extension.


# Next up, DataFusion
import datafusion
import pyarrow as pa
from substrait.proto import Plan

ctx = datafusion.SessionContext(datafusion.SessionConfig())

# Manually specify schema to avoid dictionary columns
schema = pa.schema(
    [
        pa.field("species", pa.string()),
        pa.field("island", pa.string()),
        pa.field("bill_length_mm", pa.float64()),
        pa.field("bill_depth_mm", pa.float64()),
        pa.field("body_mass_g", pa.int32()),
        pa.field("sex", pa.string()),
        pa.field("year", pa.int32()),
    ]
)

ctx.register_parquet("penguins", "./data/penguins.parquet", schema=schema)

plan_bytes = datafusion.substrait.Serde.serialize_bytes(query_str, ctx)

# Convert to a proto Plan so we can look at it
p = Plan()
p.ParseFromString(plan_bytes)
p

# This looks pretty good and the only note I have is that the select projection
# on the ReadRel isn't needed.

# Now let's see if it produces the right result:
df_substrait_plan = datafusion.substrait.Serde.deserialize_bytes(plan_bytes)
df_logical_plan = datafusion.substrait.Consumer.from_substrait_plan(ctx, df_substrait_plan)
ctx.create_dataframe_from_logical_plan(df_logical_plan)

# DataFrame()
# +---------+-----------+----------------+---------------+-------------+--------+------+
# | species | island    | bill_length_mm | bill_depth_mm | body_mass_g | sex    | year |
# +---------+-----------+----------------+---------------+-------------+--------+------+
# | Adelie  | Torgersen | 39.1           | 18.7          | 3750        | male   | 2007 |
# | Adelie  | Torgersen | 39.5           | 17.4          | 3800        | female | 2007 |
# | Adelie  | Torgersen | 40.3           | 18.0          | 3250        | female | 2007 |
# | Adelie  | Torgersen |                |               |             |        | 2007 |
# | Adelie  | Torgersen | 36.7           | 19.3          | 3450        | female | 2007 |
# | Adelie  | Torgersen | 39.3           | 20.6          | 3650        | male   | 2007 |
# | Adelie  | Torgersen | 38.9           | 17.8          | 3625        | female | 2007 |
# | Adelie  | Torgersen | 39.2           | 19.6          | 4675        | male   | 2007 |
# | Adelie  | Torgersen | 34.1           | 18.1          | 3475        |        | 2007 |
# | Adelie  | Torgersen | 42.0           | 20.2          | 4250        |        | 2007 |
# +---------+-----------+----------------+---------------+-------------+--------+------+

# This looks correct.
