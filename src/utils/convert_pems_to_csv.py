"""
Simple script to convert PEMS .txt file to CSV format.

This reads the PEMS text file and converts it to CSV with proper column headers.
"""

import pandas as pd
from pathlib import Path
import argparse
import sys


def convert_pems_to_csv(input_file, output_file=None):
    """
    Convert PEMS .txt file to CSV format.
    
    Args:
        input_file: Path to PEMS .txt file
        output_file: Path to output CSV file (default: input_file with .csv extension)
    """
    print(f"Converting {input_file} to CSV...")
    
    try:
        print("  Reading file...")
        df = pd.read_csv(
            input_file,
            sep=',',
            low_memory=False,
            encoding='utf-8',
            header=None
        )
        
        first_val = str(df.iloc[0, 0]) if len(df) > 0 else ""
        
        if first_val and ('/' in first_val or '-' in first_val):
            # First row is data, not header - add column names
            print("  No header detected, adding PEMS column names...")
            
            # Base columns: 12 fixed columns
            num_cols = df.shape[1]
            
            # Base columns (12 total)
            base_columns = [
                'Timestamp', 'Station', 'District', 'Freeway #', 'Direction of Travel', 'Lane Type',
                'Station Length', 'Samples', '% Observed', 'Total Flow', 'Avg Occupancy', 'Avg Speed'
            ]
            
            # Calculate number of lanes: (total_cols - 12) / 5
            num_lanes = (num_cols - 12) // 5
            
            print(f"  Detected {num_lanes} lanes ({(num_cols - 12)} lane-specific columns)")
            
            # Build column names - only keep the base 12 columns
            column_names = base_columns.copy()
            
            # Assign column names to all columns first
            all_column_names = base_columns.copy()
            for lane_num in range(1, num_lanes + 1):
                all_column_names.extend([
                    f'Lane {lane_num} Samples',
                    f'Lane {lane_num} Flow',
                    f'Lane {lane_num} Avg Occ',
                    f'Lane {lane_num} Avg Speed',
                    f'Lane {lane_num} Observed'
                ])
            
            # Handle any extra columns
            if num_cols > len(all_column_names):
                all_column_names.extend([f'Extra_{i}' for i in range(len(all_column_names), num_cols)])
            elif num_cols < len(all_column_names):
                all_column_names = all_column_names[:num_cols]
            
            # Assign all column names temporarily
            df.columns = all_column_names
            
            # Keep only the base 12 columns
            print(f"  Keeping only base columns, removing {num_cols - 12} lane-specific columns...")
            df = df[base_columns].copy()
        else:
            print("  Header row detected, using existing column names...")
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)
        
        # Determine output file name
        if output_file is None:
            input_path = Path(input_file)
            output_file = input_path.parent / f"{input_path.stem}.csv"
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save as CSV
        print(f"  Saving to {output_path}...")
        df.to_csv(output_path, index=False)
        
        print(f"\n✓ Successfully converted to CSV")
        print(f"  Records: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        print(f"\n  Column names: {list(df.columns[:10])}..." if len(df.columns) > 10 else f"\n  Column names: {list(df.columns)}")
        print(f"\n  First few rows:")
        print(df.head(3))
        
        return df
        
    except Exception as e:
        print(f"Error converting file: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Convert PEMS .txt file to CSV format"
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to PEMS .txt file"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV file path (default: input_file with .csv extension)"
    )
    
    args = parser.parse_args()
    
    if not Path(args.input_file).exists():
        print(f"Error: File not found: {args.input_file}")
        sys.exit(1)
    
    convert_pems_to_csv(args.input_file, args.output)


if __name__ == "__main__":
    main()

