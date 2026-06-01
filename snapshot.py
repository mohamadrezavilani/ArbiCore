#!/usr/bin/env python3
"""
Project Snapshot Generator

Recursively scans the current directory and produces a single text file containing:
- The full directory tree (like the `tree` command)
- The source code / textual content of every file (excluding binaries, large files, and common ignored directories)

Usage:
    python snapshot.py

Output:
    project_snapshot.txt (in the current directory)
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# ===== CONFIGURATION =====
OUTPUT_FILE = "project_snapshot.txt"
MAX_FILE_SIZE = 2024 * 1024  # 1 MB - skip files larger than this
INDENT = "    "               # tree indentation string

# Directories to ignore (common VCS, caches, build artifacts, virtual envs)
IGNORE_DIRS = {
    ".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".tox",
    ".venv", "venv", "env", "ENV", "virtualenv",
    "node_modules", "bower_components",
    ".idea", ".vscode", ".vs", ".settings",
    "dist", "build", "target", "out", "bin", "obj",
    ".next", ".nuxt", ".svelte-kit", ".cache",
    ".terraform", ".serverless","test_images"
}

# File extensions that are definitely binary and should be skipped
# (you can add more, e.g. images, videos, archives)
BINARY_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".ico",
    ".mp3", ".mp4", ".avi", ".mov", ".mkv",
    ".zip", ".tar", ".gz", ".bz2", ".7z", ".rar",
    ".exe", ".dll", ".so", ".dylib", ".bin",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".db", ".sqlite", ".sqlite3",
    ".pyc", ".pyo", ".pyd", ".so", ".dll",
    ".class", ".jar", ".war",
    ".iso", ".img", ".vhd", ".vhdx", ".jpg", ".txt", ".pdiparams", ".pdmodel", ".yml", ".pt", ".jpeg", ".png", ".ttf", ".TTF", ".json", ".log", ".config", ".csv", ".js"
}

# ===== HELPER FUNCTIONS =====

def is_text_file(file_path: Path) -> bool:
    """
    Determine if a file is likely a text file.
    Heuristics: extension not in BINARY_EXTENSIONS, and file does not contain null bytes.
    """
    if file_path.suffix.lower() in BINARY_EXTENSIONS:
        return False
    # Additionally, peek at the first 8KB for null bytes
    try:
        with open(file_path, "rb") as f:
            sample = f.read(8192)
            if b"\0" in sample:
                return False
    except (OSError, IOError):
        return False
    return True

def should_ignore_dir(dir_path: Path) -> bool:
    """Return True if the directory should be skipped."""
    return dir_path.name in IGNORE_DIRS

def generate_tree(root_dir: Path, prefix: str = "", out_lines: list = None) -> list:
    """
    Recursively generate a tree representation of the directory structure.
    Returns a list of strings.
    """
    if out_lines is None:
        out_lines = []
    items = sorted([p for p in root_dir.iterdir() if not should_ignore_dir(p)])
    # separate directories and files for nicer tree output
    dirs = [p for p in items if p.is_dir()]
    files = [p for p in items if p.is_file()]

    for i, d in enumerate(dirs):
        is_last = (i == len(dirs) - 1) and len(files) == 0
        out_lines.append(prefix + ("└── " if is_last else "├── ") + d.name + "/")
        new_prefix = prefix + ("    " if is_last else "│   ")
        generate_tree(d, new_prefix, out_lines)

    for i, f in enumerate(files):
        is_last = (i == len(files) - 1)
        out_lines.append(prefix + ("└── " if is_last else "├── ") + f.name)

    return out_lines

def collect_text_files(root_dir: Path) -> list:
    """
    Recursively walk through the project, ignoring unwanted directories and binary files.
    Returns a sorted list of Path objects that are safe to include as text.
    """
    text_files = []
    for root, dirs, files in os.walk(root_dir):
        # Modify dirs in-place to skip ignored directories
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

        root_path = Path(root)
        for file in files:
            file_path = root_path / file
            # Skip empty files? Not needed, but we do check size and text content
            if file_path.stat().st_size > MAX_FILE_SIZE:
                print(f"Skipping {file_path} (size > {MAX_FILE_SIZE//1024} KB)")
                continue
            if is_text_file(file_path):
                text_files.append(file_path)
            else:
                print(f"Skipping binary file: {file_path}")
    # Return sorted for consistent order
    return sorted(text_files)

def write_snapshot(output_path: Path, root_dir: Path, tree_lines: list, text_files: list) -> None:
    """Write the entire snapshot (tree + file contents) to the output file."""
    with open(output_path, "w", encoding="utf-8") as out:
        out.write("=" * 80 + "\n")
        out.write(f"PROJECT SNAPSHOT - {root_dir.resolve()}\n")
        out.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out.write("=" * 80 + "\n\n")

        # Write the directory tree
        out.write("DIRECTORY STRUCTURE\n")
        out.write("==================\n")
        out.write(root_dir.name + "/\n")
        for line in tree_lines:
            out.write(line + "\n")
        out.write("\n")

        # Write each text file's content
        out.write("FILE CONTENTS\n")
        out.write("=============\n\n")
        for file_path in text_files:
            rel_path = file_path.relative_to(root_dir)
            out.write("-" * 60 + "\n")
            out.write(f"FILE: {rel_path}\n")
            out.write("-" * 60 + "\n")
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    content = f.read()
                    out.write(content)
                    if not content.endswith("\n"):
                        out.write("\n")
            except Exception as e:
                out.write(f"[ERROR reading file: {e}]\n")
            out.write("\n\n")  # extra blank line between files

def main():
    root_dir = Path.cwd()
    output_path = root_dir / OUTPUT_FILE

    # Guard against overwriting itself if run inside an existing snapshot file
    if output_path.exists():
        print(f"Warning: {OUTPUT_FILE} already exists. It will be overwritten.")

    print(f"Scanning {root_dir}...")
    print("Generating directory tree...")
    tree_lines = generate_tree(root_dir)

    print("Collecting text files (skipping binaries, large files, and ignored directories)...")
    text_files = collect_text_files(root_dir)

    print(f"Writing snapshot to {output_path}...")
    write_snapshot(output_path, root_dir, tree_lines, text_files)

    print("Done!")
    print(f"Snapshot saved to {output_path}")
    print(f"Total text files included: {len(text_files)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)