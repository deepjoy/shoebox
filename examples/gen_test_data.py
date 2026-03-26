#!/usr/bin/env python3
"""Generate a 5-level nested directory structure with ~20,000 files.

Used to reproduce and benchmark L1 scan performance.

Structure:
  {root}/
    {cat}/                         ← level 1 (5 dirs, 2 files each)
      {sub}/                       ← level 2 (4 per parent, 3 files each)
        {grp}/                     ← level 3 (4 per parent, 5 files each)
          {batch}/                 ← level 4 (4 per parent, 10 files each)
            {chunk}/               ← level 5 (4 per parent, 12 files each)

File counts:
  L1: 5 dirs × 2 files   =     10
  L2: 20 dirs × 3 files  =     60
  L3: 80 dirs × 5 files  =    400
  L4: 320 dirs × 10 files =  3,200
  L5: 1,280 dirs × 12 files = 15,360
  ────────────────────────────────
  Total dirs: 1,705 | files: ~19,030

Usage:
  python3 examples/gen_test_data.py [target_dir]
  python3 examples/gen_test_data.py /tmp/shoebox-bench
"""

import os
import sys
import time

EXTENSIONS = [
    "jpg", "jpeg", "png", "gif", "heic",   # photos
    "mp4", "mov", "avi", "mkv",             # videos
    "pdf", "doc", "docx", "txt", "md",      # documents
    "mp3", "flac", "wav", "aac",            # audio
    "zip", "tar", "gz", "bz2",             # archives
    "rs", "py", "js", "ts", "go",          # code
]

CATEGORIES    = ["photos", "videos", "documents", "music", "archives"]
SUBCATEGORIES = ["personal", "work", "shared", "backup"]
GROUPS        = ["2021", "2022", "2023", "2024"]
BATCHES       = ["q1", "q2", "q3", "q4"]
CHUNKS        = ["a", "b", "c", "d"]


def make_files(directory: str, count: int, offset: int = 0) -> int:
    ext_count = len(EXTENSIONS)
    for i in range(count):
        idx = offset + i
        ext = EXTENSIONS[idx % ext_count]
        path = os.path.join(directory, f"file_{idx:05d}.{ext}")
        # Write a small amount of content so stat() sees non-zero sizes.
        with open(path, "wb") as f:
            f.write(b"\x00" * ((idx % 256) + 1))
    return count


def generate(root: str) -> tuple[int, int]:
    os.makedirs(root, exist_ok=True)
    total_files = 0
    total_dirs  = 0
    file_offset = 0

    for cat in CATEGORIES:
        l1 = os.path.join(root, cat)
        os.makedirs(l1, exist_ok=True)
        total_dirs += 1
        file_offset += make_files(l1, 2, file_offset)

        for sub in SUBCATEGORIES:
            l2 = os.path.join(l1, sub)
            os.makedirs(l2, exist_ok=True)
            total_dirs += 1
            file_offset += make_files(l2, 3, file_offset)

            for grp in GROUPS:
                l3 = os.path.join(l2, grp)
                os.makedirs(l3, exist_ok=True)
                total_dirs += 1
                file_offset += make_files(l3, 5, file_offset)

                for batch in BATCHES:
                    l4 = os.path.join(l3, batch)
                    os.makedirs(l4, exist_ok=True)
                    total_dirs += 1
                    file_offset += make_files(l4, 10, file_offset)

                    for chunk in CHUNKS:
                        l5 = os.path.join(l4, chunk)
                        os.makedirs(l5, exist_ok=True)
                        total_dirs += 1
                        file_offset += make_files(l5, 12, file_offset)

    total_files = file_offset
    return total_dirs, total_files


def main() -> None:
    root = sys.argv[1] if len(sys.argv) > 1 else "/tmp/shoebox-bench"

    if os.path.exists(root) and os.listdir(root):
        print(f"[warn] {root} already exists and is non-empty — files will be overwritten")

    print(f"Generating test data in: {root}")
    t0 = time.monotonic()
    dirs, files = generate(root)
    elapsed = time.monotonic() - t0

    print(f"Done in {elapsed:.2f}s")
    print(f"  Directories : {dirs:,}")
    print(f"  Files       : {files:,}")
    print()
    print(f"Run the L1 scan against this bucket with:")
    print(f"  cargo run --release -- {root}")
    print()
    print(f"Or profile it with the existing example:")
    print(f"  cargo run --release --example profile_l1 -- {root}")


if __name__ == "__main__":
    main()
