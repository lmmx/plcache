#!/bin/bash

CI_DIR="stubs"
CHECKSUM_DIR="$CI_DIR/checksums"
COMPRESSED_ARCHIVE="$CI_DIR/venv.tar.gz"
TEMP_VENV="$CI_DIR/temp-venv"
VENV_STATE_CHECKSUM="$CI_DIR/venv_state.checksum"

mkdir -p "$CI_DIR" "$CHECKSUM_DIR"

# Check if venv state has changed
echo "Checking venv state..."
current_venv_checksum=$(find .venv -type f \( -name "*.py" -o -name "*.so" -o -name "*.so.*" -o -name "pyvenv.cfg" \) -exec sha256sum {} \; | sort | sha256sum | cut -d' ' -f1)

if [ -f "$VENV_STATE_CHECKSUM" ] && [ -f "$COMPRESSED_ARCHIVE" ] && [ "$(cat "$VENV_STATE_CHECKSUM")" = "$current_venv_checksum" ]; then
    echo "✓ Venv unchanged, using existing archive"
    compressed_size=$(du -sh "$COMPRESSED_ARCHIVE" | cut -f1)
    echo "Archive size: $compressed_size"
    exit 0
fi

# Always start with a fresh copy of the current .venv
echo "Creating temp venv from current .venv..."
rm -rf "$TEMP_VENV"
cp -r .venv "$TEMP_VENV"

# Fix the Python symlinks by copying the ENTIRE Python installation
echo "Making venv relocatable with Python 3.13..."

if [ -L "$TEMP_VENV/bin/python" ]; then
    echo "Converting Python symlinks and copying full Python installation..."
    
    # Find the real Python executable and its installation directory
    REAL_PYTHON=$(readlink -f "$TEMP_VENV/bin/python")
    PYTHON_INSTALL_DIR=$(dirname "$(dirname "$REAL_PYTHON")")
    
    echo "Python installation at: $PYTHON_INSTALL_DIR"
    
    if [ -d "$PYTHON_INSTALL_DIR" ]; then
        # Remove symlinks
        rm "$TEMP_VENV/bin/python"* 2>/dev/null || true
        
        # Copy the entire Python installation into the venv
        mkdir -p "$TEMP_VENV/python-install"
        rsync -a --exclude='lib/python3.13/ensurepip/_bundled/*.whl' \
                 --exclude='lib/python3.13/ensurepip' \
                 "$PYTHON_INSTALL_DIR"/ "$TEMP_VENV/python-install/"
        
        # Create new executables that use the copied Python with proper venv setup
        cat > "$TEMP_VENV/bin/python" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$(dirname "$SCRIPT_DIR")"
VIRTUAL_ENV="$VENV_DIR" PYTHONPATH="$VENV_DIR/lib/python3.13/site-packages" exec "$VENV_DIR/python-install/bin/python3.13" "$@"
EOF
        chmod +x "$TEMP_VENV/bin/python"
        
        # Create symlinks
        cd "$TEMP_VENV/bin"
        ln -sf python python3
        ln -sf python python3.13
        cd - >/dev/null
        
        echo "✓ Python 3.13 installation copied and made relocatable"
    else
        echo "✗ Could not find Python installation directory"
        exit 1
    fi
fi

# Update pyvenv.cfg with placeholder paths (will be fixed during extraction)
if [ -f "$TEMP_VENV/pyvenv.cfg" ]; then
    cat > "$TEMP_VENV/pyvenv.cfg" << EOF
home = PLACEHOLDER_DIR/python-install/bin
include-system-site-packages = false
version = 3.13.3
executable = PLACEHOLDER_DIR/python-install/bin/python3.13
command = PLACEHOLDER_DIR/python-install/bin/python3.13 -m venv PLACEHOLDER_DIR
EOF
    echo "✓ pyvenv.cfg created with placeholders"
fi

# Test before compression
echo "Testing venv before compression..."
if "$TEMP_VENV/bin/python" -c "import sys; print('Python version:', sys.version)" 2>/dev/null; then
    echo "✓ Python wrapper works"
else
    echo "✗ Python wrapper broken"
    exit 1
fi

# Now compress .so files only (skip Python core libraries that UPX can't handle)
echo "Compressing shared libraries..."
find "$TEMP_VENV" -name "*.so" -o -name "*.so.*" | while read file; do
    [ -f "$file" ] || continue
    
    # Skip Python core libraries that UPX can't compress
    if echo "$file" | grep -q -E "(libpython|python-install/bin/)"; then
        echo "Skipping Python core file: $(basename "$file")"
        continue
    fi
    
    # Skip if it's already been processed
    rel_path="${file#$TEMP_VENV/}"
    safe_filename=$(echo "$rel_path" | sed 's|/|_|g' | sed 's|\.|_|g')
    checksum_file="$CHECKSUM_DIR/${safe_filename}.checksum"
    current_checksum=$(sha256sum "$file" | cut -d' ' -f1)
    
    if [ -f "$checksum_file" ] && [ "$(cat "$checksum_file")" = "$current_checksum" ]; then
        echo "✓ $rel_path unchanged, skipping compression"
        continue
    fi
    
    echo "Compressing $rel_path..."
    cp "$file" "$file.backup"
    
    if upx --best "$file" 2>/dev/null && upx -t "$file" 2>/dev/null; then
        echo "✓ $rel_path compressed successfully"
        echo "$current_checksum" > "$checksum_file"
        rm "$file.backup"
    else
        echo "✗ $rel_path compression failed, reverting..."
        mv "$file.backup" "$file"
    fi
done

# Final test
echo "Testing compressed venv..."
if "$TEMP_VENV/bin/python" -c "import polars, diskcache, pytest; print('All imports OK')" 2>/dev/null; then
    echo "✓ Compressed venv works, creating archive..."
    tar -czf "$COMPRESSED_ARCHIVE" -C "$CI_DIR" temp-venv/ --transform 's/^temp-venv/venv/'
    echo "✓ Created $COMPRESSED_ARCHIVE"
    
    # Save the venv state checksum for future runs
    echo "$current_venv_checksum" > "$VENV_STATE_CHECKSUM"
    
    # Show size savings
    original_size=$(du -sh .venv | cut -f1)
    compressed_size=$(du -sh "$COMPRESSED_ARCHIVE" | cut -f1)
    echo "Size: $original_size -> $compressed_size"
else
    echo "✗ Compressed venv broken"
    # Show what went wrong
    "$TEMP_VENV/bin/python" -c "import sys; print('Python works, version:', sys.version)"
    exit 1
fi

rm -rf "$TEMP_VENV"
