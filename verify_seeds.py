#!/usr/bin/env python
"""
Verification script for dbt seed files.
Validates that all 6 dimension seed files exist, have correct headers, and include Unknown sentinel rows.
"""

import csv
import os
from pathlib import Path

SEED_DIR = Path("dbt/seeds")
REQUIRED_SEEDS = {
    "branch.csv": {"headers": ["B_ID", "B_Name"], "has_unknown": True},
    "category.csv": {"headers": ["C_ID", "C_Name"], "has_unknown": True},
    "channel.csv": {"headers": ["S_ID", "S_Channel"], "has_unknown": True},
    "country.csv": {"headers": ["C_ID", "Country"], "has_unknown": True},
    "product.csv": {"headers": ["I_ID", "I_Type"], "has_unknown": True},
    "region.csv": {"headers": ["R_ID", "Region"], "has_unknown": True},
}

def verify_seed_file(seed_path, expected_headers):
    """Verify a single seed file has correct headers and Unknown sentinel row."""
    if not seed_path.exists():
        return False, f"File does not exist: {seed_path}"
    
    try:
        with open(seed_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Verify headers
            if reader.fieldnames != expected_headers:
                return False, f"Headers mismatch. Expected {expected_headers}, got {reader.fieldnames}"
            
            # Read all rows
            rows = list(reader)
            
            if not rows:
                return False, "File is empty"
            
            # Check for Unknown sentinel row
            has_unknown = any(row.get("B_ID") == "-1" or row.get("C_ID") == "-1" or 
                            row.get("S_ID") == "-1" or row.get("R_ID") == "-1" or 
                            row.get("I_ID") == "-1" or 
                            row.get("B_Name") == "Unknown" or row.get("C_Name") == "Unknown" or
                            row.get("S_Channel") == "Unknown" or row.get("Country") == "Unknown" or
                            row.get("Region") == "Unknown" or row.get("I_Type") == "Unknown"
                            for row in rows)
            
            if not has_unknown:
                return False, "Missing Unknown sentinel row"
            
            # Count real data rows (excluding Unknown)
            data_rows = [r for r in rows if r.get("B_Name") != "Unknown" and 
                        r.get("C_Name") != "Unknown" and r.get("S_Channel") != "Unknown" and
                        r.get("Country") != "Unknown" and r.get("Region") != "Unknown" and
                        r.get("I_Type") != "Unknown"]
            
            return True, f"OK - {len(data_rows)} data rows + 1 Unknown sentinel row"
    
    except Exception as e:
        return False, f"Error reading file: {str(e)}"

def main():
    """Run verification on all seed files."""
    print("=" * 80)
    print("dbt Seed File Verification")
    print("=" * 80)
    
    all_passed = True
    
    for seed_name, config in REQUIRED_SEEDS.items():
        seed_path = SEED_DIR / seed_name
        passed, message = verify_seed_file(seed_path, config["headers"])
        
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {seed_name:20s} - {message}")
        
        if not passed:
            all_passed = False
    
    print("=" * 80)
    
    if all_passed:
        print("✓ All seed files are valid and ready for `dbt seed --target dev`")
        print("\nNext step: Run `dbt seed --target dev` to load seeds into PostgreSQL")
        return 0
    else:
        print("✗ Some seed files have issues - see details above")
        return 1

if __name__ == "__main__":
    exit(main())
