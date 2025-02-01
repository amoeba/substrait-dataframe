import duckdb

con = duckdb.connect()
con.sql("INSTALL substrait;")
con.sql("LOAD substrait;")
con.sql("create table 'penguins' as select * from 'data/penguins.parquet';")

json = con.get_substrait_json("select island from penguins;").fetchone()[0]
print(json)

raw = con.get_substrait(
    "select species from penguins where island = 'Dream';"
).fetchone()[0]
print(raw)


from substrait.proto import Plan

p = Plan()
p.ParseFromString(raw)
print(p)
