precommit:    lint
prepush:      lint             fmt taplo-check eof-check
precommit-ci: ty-ci ruff-check fmt taplo-check eof-check

lint: ty check
check: flake ruff-check
fmt: ruff-fmt taplo-fix eof-fix

setup:
   #!/usr/bin/env bash
   uv venv
   source .venv/bin/activate
   uv sync

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
          --exclude ".git/|.stubs/|.*cache/|.db|.parquet|.pdm-build|.venv/|site/|.json$|.lock|.sw[op]|.png|.jpg$" \
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

refresh-stubs:
    #!/usr/bin/env bash
    rm -rf .stubs
    set -e  # Exit on any error
    uv sync --no-group debug
    ./stub_gen.py
    deactivate
    mv .venv/ offvenv
    pre-commit run --all-files
    rm -rf .venv
    mv offvenv .venv








ty-ci:
    #!/usr/bin/env bash
    set -e  # Exit on any error
    
    echo "🔍 CI Environment Debug Information"
    echo "Current directory: $(pwd)"
    echo "Python available: $(which python3 || echo 'none')"
    echo "UV available: $(which uv || echo 'none')"
    
    # Check if .venv exists, if not extract from compressed CI venv
    if [ ! -d ".venv" ]; then
        echo "📦 Extracting compressed stubs for CI..."
        if [ -f ".stubs/venv.tar.gz" ]; then
            echo "Found compressed stubs, extracting..."
            tar -xzf .stubs/venv.tar.gz
            mv venv .venv
            
            # Fix pyvenv.cfg with current absolute path
            if [ -f ".venv/pyvenv.cfg" ]; then
                CURRENT_DIR=$(pwd)
                sed -i "s|PLACEHOLDER_DIR|${CURRENT_DIR}/.venv|g" ".venv/pyvenv.cfg"
                echo "✓ pyvenv.cfg updated with current directory: $CURRENT_DIR"
                echo "Updated pyvenv.cfg contents:"
                cat ".venv/pyvenv.cfg"
            fi
            
            echo "✅ Extraction complete, running diagnostics..."
            
            # Diagnostic checks
            echo "🔍 Venv structure check:"
            ls -la .venv/ | head -5
            echo ""
            
            echo "🔍 Python interpreter check:"
            if [ -f ".venv/bin/python" ]; then
                echo "Python executable exists"
                .venv/bin/python --version || echo "❌ Python version check failed"
            else
                echo "❌ No Python executable found"
                ls -la .venv/bin/ | head -5
            fi
            
            echo "🔍 Site-packages check:"
            SITE_PACKAGES=".venv/lib/python*/site-packages"
            if ls $SITE_PACKAGES >/dev/null 2>&1; then
                echo "Site-packages directory exists:"
                ls $SITE_PACKAGES | grep -E "(polars|diskcache)" || echo "❌ Key packages not found"
            else
                echo "❌ No site-packages directory found"
            fi
            
            echo "🔍 Environment activation test:"
            export PATH="$(pwd)/.venv/bin:$PATH"
            export VIRTUAL_ENV="$(pwd)/.venv"
            export PYTHONPATH=""  # Clear any existing PYTHONPATH
            
            echo "Active Python: $(which python)"
            python --version || echo "❌ Python activation failed"
            
            echo "🔍 Critical imports test:"
            python -c 'import sys; print("✓ Python sys module working"); print("Python executable:", sys.executable)' || echo "❌ Basic Python test failed"
            python -c 'import polars as pl; print("✓ Polars import successful, version:", pl.__version__)' || echo "❌ Polars import failed"
            python -c 'import diskcache; print("✓ Diskcache import successful")' || echo "❌ Diskcache import failed"
            python -c 'import pytest; print("✓ Pytest import successful")' || echo "❌ Pytest import failed"
            
        else
            echo "❌ No stubs found, running regular setup..."
            just setup
        fi
    else
        echo "✅ .venv already exists, activating..."
        export PATH="$(pwd)/.venv/bin:$PATH"
        export VIRTUAL_ENV="$(pwd)/.venv"
    fi
    
    echo "🚀 Running ty check..."
    just ty .
