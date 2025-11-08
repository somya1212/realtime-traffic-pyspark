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
    while True:
        src = files[i % len(files)]
        dest = out_dir / f"chunk_{int(time.time())}_{src.name}"
        shutil.copy(src, dest)
        print(f"Dropped {dest}")
        time.sleep(2)
        i += 1
