import time, shutil
from pathlib import Path

in_dir = Path("data/seeds")
out_dir = Path("data/incoming_stream")
out_dir.mkdir(parents=True, exist_ok=True)

files = sorted(in_dir.glob("*.csv"))
if not files:
    print("Put a few CSVs inside data/seeds first.")
else:
    i = 0
    max_files = 10  # keep only the last 10 files in incoming_stream
    
    while True:
        src = files[i % len(files)]
        dest = out_dir / f"chunk_{int(time.time())}_{src.name}"
        shutil.copy(src, dest)
        print(f"Dropped {dest}")
        
        # clean up old files to prevent storage from filling up
        existing_files = sorted(out_dir.glob("chunk_*.csv"), key=lambda p: p.stat().st_mtime)
        if len(existing_files) > max_files:
            # delete oldest files, keeping only the most recent max_files
            for old_file in existing_files[:-max_files]:
                try:
                    old_file.unlink()
                    print(f"Removed old file: {old_file.name}")
                except Exception as e:
                    print(f"Could not remove {old_file.name}: {e}")
        
        time.sleep(20)  
        i += 1
