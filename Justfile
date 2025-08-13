lint: ty flake

flake:
   flake8 src/plcache --max-line-length=88 --extend-ignore=E203,E501,

ty *args:
   #!/usr/bin/env bash
   ty check {{args}} 2> >(grep -v "WARN ty is pre-release software" >&2)

t:
   just ty --output-format=concise

fmt:
   ruff format src/plcache

setup:
   uv venv
   source .venv/bin/activate
   uv pip install -e .
