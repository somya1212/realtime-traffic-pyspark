# split large csv file into smaller chunks for streaming simulator

import pandas as pd
from pathlib import Path
import argparse
import sys


def create_chunks(input_file, chunk_size=5000):
    print(f"Splitting {input_file} into chunks...")
    
    output_dir = "data/seeds"
    prefix = "chunk"
    
    try:
        print(f"  Reading CSV file...")
        df = pd.read_csv(input_file, low_memory=False)
        
        total_rows = len(df)
        num_chunks = (total_rows + chunk_size - 1) // chunk_size  
        
        print(f"  Total rows: {total_rows:,}")
        print(f"  Chunk size: {chunk_size:,} rows")
        print(f"  Number of chunks: {num_chunks}")
        
        # output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # get base filename without extension
        input_path = Path(input_file)
        base_name = input_path.stem
        
        print(f"\n  Creating chunks...")
        chunk_files = []
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            
            # extract chunk
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            chunk_filename = f"{prefix}_{i+1:04d}_{base_name}.csv"
            chunk_filepath = output_path / chunk_filename
            
            # save chunk
            chunk_df.to_csv(chunk_filepath, index=False)
            chunk_files.append(chunk_filepath)
            
            print(f"    Created {chunk_filename} ({len(chunk_df):,} rows)")
        
        print(f"\nSuccessfully created {num_chunks} chunks!")
        print(f"  Output directory: {output_path}")
        print(f"  Total files created: {len(chunk_files)}")
        print(f"  Total size: {sum(f.stat().st_size for f in chunk_files) / (1024*1024):.2f} MB")
        
        print(f"\n  First few chunk files:")
        for f in chunk_files[:5]:
            print(f"    - {f.name}")
        if len(chunk_files) > 5:
            print(f"    ... and {len(chunk_files) - 5} more")
        
        return chunk_files
        
    except Exception as e:
        print(f"Error creating chunks: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Split CSV file into smaller chunks for streaming"
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to input CSV file"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of rows per chunk (default: 5000)"
    )
    
    args = parser.parse_args()
    
    if not Path(args.input_file).exists():
        print(f"Error: File not found: {args.input_file}")
        sys.exit(1)
    
    create_chunks(args.input_file, args.chunk_size)


if __name__ == "__main__":
    main()