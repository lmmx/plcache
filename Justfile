default:      lint

precommit:    lint    fmt taplo-check eof-check
precommit-ci: lint-ci fmt taplo-check eof-check

prepush: precommit test

lint:    ty    pf-miss-attr   check
lint-ci: ty-ci pf-miss-attr ruff-check
check: flake ruff-check
fmt: ruff-fmt taplo-fix eof-fix

install-hooks:
   pre-commit install

run-pc:
   pre-commit run --all-files

setup:
   #!/usr/bin/env bash
   uv venv
   source .venv/bin/activate
   uv sync

test *args:
   $(uv python find) -m pytest tests {{args}}

ty *args:
   #!/usr/bin/env bash
   ty check {{args}} 2> >(grep -v "WARN ty is pre-release software" >&2)

t:
   just ty --output-format=concise

pyrefly *args:
   #!/usr/bin/env bash
   pyrefly check {{args}}

pf *args:
   just pyrefly --output-format=min-text {{args}}

pf-miss-attr:
    #!/usr/bin/env bash
    if pyrefly check src/plcache/ --output-format=min-text 2>&1 | rg -q "\[missing-attribute\]"; then
        echo "ERROR: Found missing-attribute errors" >&2
        exit 1
    else
        echo "pyrefly: [missing-attribute] check OK"
        exit 0
    fi

flake:
   flake8 src/plcache --max-line-length=88 --extend-ignore=E203,E501,

ruff-check mode="":
   ruff check . {{mode}}

ruff-fix:
   just ruff-check --fix

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
          --exclude ".git/|.stubs/|.*cache/|.db|dist/|.parquet|.pdm-build|.venv/|site/|.json$|.lock|.sw[op]|.png|.jpg$" \
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

refresh-stubs *args="":
    #!/usr/bin/env bash
    rm -rf .stubs
    set -e  # Exit on any error
    
    # Check if --debug flag is passed and export DEBUG_PYSNOOPER
    debug_flag=false
    uv_args="--no-group debug"
    echo "Args received: {{args}}"
    if [[ "{{args}}" == *"--debug"* ]]; then
        export DEBUG_PYSNOOPER=true
        echo "DEBUG MODE: ON"
        debug_flag=true
        uv_args=""  # Remove --no-group debug when in debug mode
    fi
    
    uv sync $uv_args
    ./stub_gen.py
    deactivate
    mv .venv/ offvenv
    just run-pc
    rm -rf .venv
    mv offvenv .venv
    
    # Unset DEBUG_PYSNOOPER if it was set
    if [[ "$debug_flag" == "true" ]]; then
        unset DEBUG_PYSNOOPER
    fi

# Release a new version, pass --help for options to `pdm bump`
release bump_level="patch":
    #!/usr/bin/env bash
    set -e  # Exit on any error
    
    # Exit early if help was requested
    if [[ "{{bump_level}}" == "--help" ]]; then
        exit 0
    fi

    uv version --bump {{bump_level}}
    
    git add --all
    git commit -m "chore(temp): version check"
    new_version=$(uv version --short)
    git reset --soft HEAD~1
    git add --all
    git commit -m  "chore(release): bump -> v$new_version"
    branch_name=$(git rev-parse --abbrev-ref HEAD);
    git push origin $branch_name
    uv build
    uv publish -u __token__ -p $(keyring get PYPIRC_TOKEN "")


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


# Show all open GitHub issues with full bodies, or a specific issue number if provided
issues number="" *args:
    #!/usr/bin/env bash
    if [ -n "{{number}}" ]; then
        gh issue view "{{number}}" --json number,title,body \
            --jq '"\n" + "=" * 60 + "\nISSUE #\(.number): \(.title)\n" + "=" * 60 + "\n\n\(.body)\n\n" + "-" * 60 + "\n"'
    else
        gh issue list {{args}} --state open \
            --json number,title,body \
            --jq '.[] | "\n" + "=" * 60 + "\nISSUE #\(.number): \(.title)\n" + "=" * 60 + "\n\n\(.body)\n\n" + "-" * 60 + "\n"'
    fi

# Search for issues by title text, then display using the issues recipe
issue search_text:
    #!/usr/bin/env bash
    # Find issue number(s) that match the search text in title
    issue_numbers=$(gh issue list --state open --search "{{search_text}} in:title" --json number --jq '.[].number')
    
    if [ -z "$issue_numbers" ]; then
        echo "No issues found matching '{{search_text}}'"
        exit 1
    fi
    
    # Pass each found issue number to the issues recipe
    for num in $issue_numbers; do
        just issues "$num"
    done
