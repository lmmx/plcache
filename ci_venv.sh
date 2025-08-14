#!/bin/bash

CI_DIR=".ci_venv"
CHECKSUM_DIR="$CI_DIR/checksums"
COMPRESSED_ARCHIVE="$CI_DIR/venv.tar.gz"
TEMP_VENV="$CI_DIR/temp-venv"

mkdir -p "$CI_DIR" "$CHECKSUM_DIR"

# Extract existing tarball if it exists, otherwise copy original
if [ -f "$COMPRESSED_ARCHIVE" ]; then
    echo "Extracting existing compressed venv..."
    tar -xzf "$COMPRESSED_ARCHIVE" -C "$CI_DIR"
    mv "$CI_DIR/venv" "$TEMP_VENV"
else
    echo "No existing compressed venv, starting fresh..."
    cp -r .venv "$TEMP_VENV"
fi

# Process each binary file
{
    find .venv -type f \( -name "*.so" -o -name "*.so.*" \)
    find .venv -type f -executable -exec file {} \; | grep -E "(ELF|PE32|Mach-O)" | cut -d: -f1
} | while read file; do
    [ -f "$file" ] || continue
    
    current_checksum=$(sha256sum "$file" | cut -d' ' -f1)
    safe_filename=$(echo "$file" | sed 's|/|_|g' | sed 's|\.|_|g')
    checksum_file="$CHECKSUM_DIR/${safe_filename}.checksum"
    temp_file="$TEMP_VENV/${file#.venv/}"
    
    if [ -f "$checksum_file" ] && [ "$(cat "$checksum_file")" = "$current_checksum" ]; then
        echo "✓ $file unchanged, keeping existing compressed version"
    else
        echo "Processing $file..."
        # Ensure directory exists
        mkdir -p "$(dirname "$temp_file")"
        # Copy new version and compress it
        cp "$file" "$temp_file"
        upx --best "$temp_file"
        
        if upx -t "$temp_file"; then
            echo "✓ $file compressed successfully"
            echo "$current_checksum" > "$checksum_file"
        else
            echo "✗ $file failed test, reverting..."
            upx -d "$temp_file"
        fi
    fi
done

# Test and create new tarball
echo "Testing compressed venv..."
if "$TEMP_VENV/bin/python" -c "import polars, diskcache, pytest; print('All imports OK')" 2>/dev/null; then
    echo "✓ Compressed venv works, creating archive..."
    tar -czf "$COMPRESSED_ARCHIVE" -C "$CI_DIR" temp-venv/ --transform 's/^temp-venv/venv/'
    echo "✓ Created $COMPRESSED_ARCHIVE"
else
    echo "✗ Compressed venv broken"
    rm -rf "$TEMP_VENV"
    exit 1
fi

rm -rf "$TEMP_VENV"