import duckdb

con = duckdb.connect()
con.sql("INSTALL substrait;")
con.sql("LOAD substrait;")
con.sql("CREATE TABLE 'penguins' AS SELECT * FROM 'data/penguins.parquet';")

with open("tools/select-star-from-penguins.json", "w") as f:
    f.writelines(con.get_substrait_json("select * from penguins;").fetchone()[0])


with open("tools/select-island-from-penguins.json", "w") as f:
    f.writelines(con.get_substrait_json("select island from penguins;").fetchone()[0])

with open("tools/select-species-from-penguins-where-island-dream.json", "w") as f:
    f.writelines(
        con.get_substrait_json(
            "select species from penguins where island = 'Dream';"
        ).fetchone()[0]
    )
