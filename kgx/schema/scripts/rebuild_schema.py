#!/usr/bin/env python3
"""
Rebuild KGX Schema Script

This script orchestrates the process of building the complete KGX schema:
1. Starts with core kgx.yaml that imports biolink
2. Materializes the imports to create a merged schema
3. Generates a transformation spec for adding inheritance relationships
4. Applies the transformation to create the final complete schema

Usage:
  python rebuild_schema.py
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

# Add parent directory to python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

# Define paths
SCHEMA_DIR = Path(__file__).resolve().parent.parent
ROOT_DIR = SCHEMA_DIR.parent.parent
CORE_SCHEMA = SCHEMA_DIR / "kgx.yaml"
MERGED_SCHEMA = SCHEMA_DIR / "kgx_merged.yaml"
TRANSFORM_SPEC = SCHEMA_DIR / "transformations" / "full_transform.transform.yaml"
COMPLETE_SCHEMA = SCHEMA_DIR / "kgx_complete.yaml"

# Clean up old files
def clean_up():
    """Remove intermediate files from previous runs"""
    print("Cleaning up old files...")
    
    # Files to keep
    keep_files = [
        SCHEMA_DIR / "kgx.yaml",
        SCHEMA_DIR / "transformations" / "kgx_inheritance.transform.yaml",
    ]
    
    # Get all yaml files in the schema directory
    yaml_files = list(SCHEMA_DIR.glob("*.yaml"))
    yaml_files.extend(SCHEMA_DIR.glob("*.yml"))
    
    # Remove files that are not in the keep list
    for file_path in yaml_files:
        if file_path not in keep_files and file_path.name != "kgx.yaml":
            print(f"  Removing {file_path.name}")
            file_path.unlink(missing_ok=True)

def run_command(cmd, description):
    """Run a shell command and handle errors"""
    print(f"{description}...")
    try:
        subprocess.run(cmd, check=True)
        print(f"  {description} completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"  Error during {description}: {e}")
        sys.exit(1)

def main():
    """Main function to orchestrate the rebuild process"""
    print("Starting KGX schema rebuild process")
    
    # Step 1: Clean up old files
    clean_up()
    
    # Step 2: Materialize imports to create merged schema
    run_command(
        ["poetry", "run", "gen-linkml", "--mergeimports", "--format", "yaml", 
         str(CORE_SCHEMA), "-o", str(MERGED_SCHEMA)],
        "Merging schema imports"
    )
    
    # Step 3: Generate transformation spec
    # Move the generate_transform.py script if it's not in the expected location
    transform_script = ROOT_DIR / "generate_transform.py"
    if transform_script.exists():
        shutil.copy(transform_script, SCHEMA_DIR / "scripts" / "generate_transform.py")
    
    run_command(
        ["poetry", "run", "python", str(SCHEMA_DIR / "scripts" / "generate_transform.py"),
         str(MERGED_SCHEMA), str(TRANSFORM_SPEC)],
        "Generating transformation specification"
    )
    
    # Step 4: Apply transformation to create complete schema
    run_command(
        ["poetry", "run", "linkml-map", "derive-schema", 
         "-T", str(TRANSFORM_SPEC), "-o", str(COMPLETE_SCHEMA), str(MERGED_SCHEMA)],
        "Applying transformation"
    )
    
    print(f"\nCompleted! Final schema is available at: {COMPLETE_SCHEMA}")

if __name__ == "__main__":
    main()