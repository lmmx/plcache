## Stubs

The project aims to use stubs, initially these are just the entire package and progressively being
reduced to stubs. The stubs directory contains a compressed tarball filled with `upx` compressed
static object binaries to save space. Run `just refresh-stubs` to refresh them from the current
`.venv` (it will run `uv sync` to do this). This is necessary to run type checking on airgapped CI.

To debug with pysnooper, `export DEBUG_PYSNOOPER=true` and manually remove the `--no-group debug`
from the recipe, or just call the script directly (see the Justfile `refresh-stubs` recipe for details).
