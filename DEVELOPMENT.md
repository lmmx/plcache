## Just

The task runner file is `Justfile` and all the development tasks are coordinated through running
`just` plus the name of the "recipe" (small script). It's pretty self-explanatory.

See [docs](https://github.com/casey/just).

## Precommit

To run the linters, install [pre-commit](https://pre-commit.com/) (globally on your system,
typically) and then `pre-commit install` to set the git commit hooks. You can also run the hooks
manually but development is easier with them installed and running automatically.

```sh
just install-hooks
```

## Stubs

The project aims to use stubs, initially these are just the entire package and progressively being
reduced to stubs. The stubs directory contains a compressed tarball filled with `upx` compressed
static object binaries to save space.

To refresh them from the current `.venv` (it will run `uv sync` to do this) run:

```sh
just refresh-stubs
```

This is necessary to run type checking on airgapped CI.

To debug with pysnooper, `export DEBUG_PYSNOOPER=true` and manually remove the `--no-group debug`
from the recipe, or just call the script directly (see the Justfile `refresh-stubs` recipe for details).
