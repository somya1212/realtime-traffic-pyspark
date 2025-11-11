"""
Single script to prepare PEMS data for streaming pipeline.

This script combines all data preparation steps:
1. Remove lane-specific columns
2. Clean data (remove invalid rows)
3. Transform timestamp format
4. Add region column

Takes the converted CSV (from convert_pems_to_csv.py) and produces final transformed data.
"""

import pandas as pd
from pathlib import Path
import argparse
import sys


def prepare_final_data(input_file, output_file=None):
    """
    Prepare PEMS data for streaming: remove lanes, clean, and transform.
    
    Args:
        input_file: Path to converted PEMS CSV file (with all columns)
        output_file: Path to output CSV (default: input_file with _final suffix)
    """
    print(f"Preparing final data from {input_file}...")
    
    try:
        df = pd.read_csv(input_file, low_memory=False)
        
        print("\n  Step 1: Removing lane-specific columns...")
        base_columns = [
            'Timestamp', 'Station', 'District', 'Freeway #', 'Direction of Travel', 'Lane Type',
            'Station Length', 'Samples', '% Observed', 'Total Flow', 'Avg Occupancy', 'Avg Speed'
        ]
        
        # Find which base columns exist in the file
        columns_to_keep = [col for col in base_columns if col in df.columns]
        
        # Remove all lane-specific columns
        lane_columns = [col for col in df.columns if col.startswith('Lane ') or col.startswith('Extra_')]
        
        print(f"    Removing {len(lane_columns)} lane-specific columns...")
        print(f"    Keeping {len(columns_to_keep)} base columns")
        
        # Keep only base columns
        df_cleaned = df[columns_to_keep].copy()
        
        # Step 2: Clean the data
        print("\n  Step 2: Cleaning data...")
        initial_count = len(df_cleaned)
        
        # 1. Remove rows where % Observed is 0
        if '% Observed' in df_cleaned.columns:
            before = len(df_cleaned)
            # Convert to numeric and remove rows where value is 0
            df_cleaned['% Observed'] = pd.to_numeric(df_cleaned['% Observed'], errors='coerce')
            df_cleaned = df_cleaned[df_cleaned['% Observed'] != 0].copy()
            removed_observed = before - len(df_cleaned)
            if removed_observed > 0:
                print(f"    Removed {removed_observed:,} rows where % Observed = 0")
        
        # 2. Remove rows where Total Flow is NaN
        if 'Total Flow' in df_cleaned.columns:
            before = len(df_cleaned)
            # Convert to numeric first
            df_cleaned['Total Flow'] = pd.to_numeric(df_cleaned['Total Flow'], errors='coerce')
            df_cleaned = df_cleaned[df_cleaned['Total Flow'].notna()].copy()
            removed_flow = before - len(df_cleaned)
            if removed_flow > 0:
                print(f"    Removed {removed_flow:,} rows where Total Flow is NaN")
        
        # 3. Remove rows where Avg Speed is NaN
        if 'Avg Speed' in df_cleaned.columns:
            before = len(df_cleaned)
            # Convert to numeric first
            df_cleaned['Avg Speed'] = pd.to_numeric(df_cleaned['Avg Speed'], errors='coerce')
            df_cleaned = df_cleaned[df_cleaned['Avg Speed'].notna()].copy()
            removed_speed = before - len(df_cleaned)
            if removed_speed > 0:
                print(f"    Removed {removed_speed:,} rows where Avg Speed is NaN")
        
        total_removed = initial_count - len(df_cleaned)
        if total_removed > 0:
            print(f"    Total removed: {total_removed:,} rows ({total_removed/initial_count*100:.1f}%)")
            print(f"    Remaining: {len(df_cleaned):,} rows")
        
        # Step 3: Transform timestamp format
        print("\n  Step 3: Converting timestamp format...")
        if 'Timestamp' in df_cleaned.columns:
            # Parse MM/DD/YYYY HH24:MI:SS format
            df_cleaned['Timestamp'] = pd.to_datetime(
                df_cleaned['Timestamp'], 
                format='%m/%d/%Y %H:%M:%S', 
                errors='coerce'
            )
            # Convert to YYYY-MM-DD HH:MM:SS format
            df_cleaned['Timestamp'] = df_cleaned['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            print(f"Converted timestamp format (MM/DD/YYYY → YYYY-MM-DD)")
        else:
            print("Warning: 'Timestamp' column not found")
        
        # Step 4: Add region column
        print("\n  Step 4: Adding region column...")
        if 'District' in df_cleaned.columns:
            # Map District 7 to "LA"
            df_cleaned['region'] = df_cleaned['District'].apply(
                lambda x: 'LA' if pd.notna(x) and (x == 7 or str(x) == '7') else 'LA'
            )
            print(f"Added 'region' column with value 'LA'")
            print(f"District values: {df_cleaned['District'].unique()}")
        else:
            print("Warning: 'District' column not found, adding default 'LA' region")
            df_cleaned['region'] = 'LA'
        
        # Determine output file
        if output_file is None:
            input_path = Path(input_file)
            output_file = input_path.parent / f"{input_path.stem}_final.csv"
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"\n  Saving final data to {output_path}...")
        df_cleaned.to_csv(output_path, index=False)
        
        print(f"\n✓ Successfully prepared final data!")
        print(f"  Output: {output_path}")
        print(f"  Records: {len(df_cleaned):,}")
        print(f"  Columns: {len(df_cleaned.columns)}")
        print(f"  File size: {output_path.stat().st_size / (1024*1024):.2f} MB")
        print(f"\n  Final columns: {list(df_cleaned.columns)}")
        print(f"\n  Sample data:")
        print(df_cleaned.head(3))
        
        return df_cleaned
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Prepare PEMS data for streaming: remove lanes, clean, and transform"
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to converted PEMS CSV file (from convert_pems_to_csv.py)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV file path (default: input_file with _final suffix)"
    )
    
    args = parser.parse_args()
    
    if not Path(args.input_file).exists():
        print(f"Error: File not found: {args.input_file}")
        sys.exit(1)
    
    prepare_final_data(args.input_file, args.output)


if __name__ == "__main__":
    main()

