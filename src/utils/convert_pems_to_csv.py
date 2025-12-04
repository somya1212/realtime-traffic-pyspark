# script to convert a raw PEMS .txt file into a cleaner csv 

import pandas as pd
from pathlib import Path
import argparse
import sys


def convert_pems_to_csv(input_file, output_file=None):
    """
    args:
        input_file: path to PEMS .txt file
        output_file: path to csv file (default: input_file with .csv extension)
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
        
        # check whether the file already has a header
        first_val = str(df.iloc[0, 0]) if len(df) > 0 else ""
        
        if first_val and ('/' in first_val or '-' in first_val):
            print("  No header detected, adding PEMS column names...")
            
            num_cols = df.shape[1]
            
            # base columns (12 total)
            base_columns = [
                'Timestamp', 'Station', 'District', 'Freeway #', 'Direction of Travel', 'Lane Type',
                'Station Length', 'Samples', '% Observed', 'Total Flow', 'Avg Occupancy', 'Avg Speed'
            ]
            
            # calculate number of lanes
            num_lanes = (num_cols - 12) // 5
            print(f"  Detected {num_lanes} lanes ({(num_cols - 12)} lane-specific columns)")
            
            # build full column list
            column_names = base_columns.copy()
            
            all_column_names = base_columns.copy()
            for lane_num in range(1, num_lanes + 1):
                all_column_names.extend([
                    f'Lane {lane_num} Samples',
                    f'Lane {lane_num} Flow',
                    f'Lane {lane_num} Avg Occ',
                    f'Lane {lane_num} Avg Speed',
                    f'Lane {lane_num} Observed'
                ])
            
            # handle any extra/missing columns
            if num_cols > len(all_column_names):
                all_column_names.extend([f'Extra_{i}' for i in range(len(all_column_names), num_cols)])
            elif num_cols < len(all_column_names):
                all_column_names = all_column_names[:num_cols]
            
            df.columns = all_column_names
            
            # keep only the base 12 columns
            print(f"  Keeping only base columns, removing {num_cols - 12} lane-specific columns...")
            df = df[base_columns].copy()
        else:
            print("  Header row detected, using existing column names...")
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)
        
        # output file name
        if output_file is None:
            input_path = Path(input_file)
            output_file = input_path.parent / f"{input_path.stem}.csv"
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # save as csv
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