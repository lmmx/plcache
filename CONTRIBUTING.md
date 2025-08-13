## Development Requirements

I would suggest you install:

- `uv`
- `just`

Then run

```sh
just setup
```

This will run the 'setup' recipe in `Justfile` to give you a local uv virtual environment with the package installed.

## Linting

- Run `just` to run the linter recipes (the default with no arguments).
- Run `just ty` to run `ty` type checking.
- Run `just flake` to run `flake8` linting
- Run `just fmt` to run `ruff format` auto-formatter.
