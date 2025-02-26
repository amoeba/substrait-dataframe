# substrait-dataframe

TODO

## Support Functionality

- Selecting fields
- Simple filtering on one or more fields
- Limit

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

- Producers like DuckDB and DataFusion produce use Project in a way that works but is technically wrong so it's important when basing a new implementation of an existing one to look carefully at the produced plans you reference.
- DuckDB's consumer implementation is more flexible in what it accepts whereas DataFusion's is more strict. i.e., DuckDB will successfully run plans that are technically invalid. Running serialized plans through the Substrait validator as you build is a good idea.
