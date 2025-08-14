lint: ty flake ruff-check
precommit: fmt taplo-check eof-check
fmt: ruff-fmt taplo-fix eof-fix

setup:
   uv venv
   source .venv/bin/activate
   uv pip install -e .

test:
   $(uv python find) -m pytest tests

ty *args:
   #!/usr/bin/env bash
   ty check {{args}} 2> >(grep -v "WARN ty is pre-release software" >&2)

t:
   just ty --output-format=concise

flake:
   flake8 src/plcache --max-line-length=88 --extend-ignore=E203,E501,

ruff-check mode="":
   ruff check . {{mode}}

ruff-fmt:
   ruff format .

taplo-check:
    taplo lint
    taplo format --check

taplo-fix:
    taplo lint
    taplo format

eof-check:
    just fix-eof-ws check

eof-fix:
    just fix-eof-ws

fix-eof-ws mode="":
    #!/usr/bin/env sh
    ARGS=''
    if [ "{{mode}}" = "check" ]; then
        ARGS="--check-only"
    fi
    whitespace-format --add-new-line-marker-at-end-of-file \
          --new-line-marker=linux \
          --normalize-new-line-markers \
          --exclude ".git/|.*cache/|.db|.parquet|.pdm-build|.venv/|site/|.json$|.lock|.sw[op]|.png|.jpg$" \
          $ARGS \
          .

examples: example-basic example-advanced example-perf

[working-directory: 'examples/basic']
example-basic:
   $(uv python find) basic_usage.py

[working-directory: 'examples/advanced']
example-advanced:
   $(uv python find) advanced_usage.py

[working-directory: 'examples/perf']
example-perf:
   $(uv python find) performance_comparison.py
