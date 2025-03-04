# substrait-dataframe

A minimal reference implementation of creating a Python DataFrame library that executes on any engine that speaks [Substrait](https://substrait.io).
The actual DataFrame library functionality is _very_ limited and this repository is mainly to show the integration with Substrait.

## Support Functionality

- Select: Selecting one or more fields
- Filter: Simple filtering on one or more string fields
- Limit

## Installing

Assuming a standard Python installation with access to pip:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install .
```

## Testing

Using the above venv, run:

```sh
python -m pip install .[testing]
pytest
```

## Example

There's an example in the root of the repository at `./example.py`.
