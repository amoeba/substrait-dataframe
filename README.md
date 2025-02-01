# substrait-dataframe

TODO

## Supported Substrait Features

- Reads
  - [x] Named tables
  - [ ] Virtual tables
  - [ ] Files
- [ ] Expressions
  - [x] Field references
  - [ ] Literals
  - [ ] Functions
  - [ ] Subqueries
  - [ ] Window functions

## Lessons Learned

- While building this, I found DuckDB's consumer implementation is more flexible in what it accepts whereas DataFusion's is more strict. i.e., DuckDB will successfully run plans that are technically invalid. Running serialized plans through the Substrait validator as you build is a good idea.
