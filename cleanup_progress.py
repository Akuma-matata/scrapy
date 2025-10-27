"""
Cleanup Progress Script
Removes failed downloads from progress.json so they can be retried.
"""

import json
from pathlib import Path

def cleanup_progress():
    """Remove failed run IDs from progress.json"""

    # File paths
    progress_file = Path("Healthy_Data/progress.json")
    failed_log = Path("Healthy_Data/failed_downloads.txt")

    # Check if files exist
    if not progress_file.exists():
        print(f"Error: {progress_file} not found")
        return

    if not failed_log.exists():
        print(f"Error: {failed_log} not found")
        return

    # Load progress.json
    print(f"Loading {progress_file}...")
    with open(progress_file, 'r') as f:
        progress_data = json.load(f)

    original_completed = set(progress_data.get("completed", []))
    print(f"Original completed count: {len(original_completed)}")

    # Read failed downloads
    print(f"Reading {failed_log}...")
    failed_runs = set()
    with open(failed_log, 'r') as f:
        for line in f:
            # Format: run_id\terror_msg\ttimestamp
            parts = line.strip().split('\t')
            if parts:
                run_id = parts[0]
                failed_runs.add(run_id)

    print(f"Found {len(failed_runs)} failed downloads")

    # Remove failed runs from completed list
    cleaned_completed = original_completed - failed_runs
    removed_count = len(original_completed) - len(cleaned_completed)

    print(f"Removing {removed_count} failed runs from progress...")

    # Update progress data
    progress_data["completed"] = list(cleaned_completed)
    progress_data["downloaded_count"] = len(cleaned_completed)

    # Backup original file
    backup_file = progress_file.with_suffix('.json.backup')
    print(f"Creating backup at {backup_file}...")
    with open(backup_file, 'w') as f:
        json.dump(progress_data, f, indent=2)

    # Save updated progress
    print(f"Saving cleaned progress to {progress_file}...")
    with open(progress_file, 'w') as f:
        json.dump(progress_data, f, indent=2)

    # Print summary
    print("\n" + "="*60)
    print("CLEANUP SUMMARY")
    print("="*60)
    print(f"Original completed runs: {len(original_completed)}")
    print(f"Failed runs found: {len(failed_runs)}")
    print(f"Removed from progress: {removed_count}")
    print(f"New completed count: {len(cleaned_completed)}")
    print("="*60)
    print(f"\nBackup saved to: {backup_file}")
    print("You can now re-run the downloader to retry failed downloads.")

if __name__ == "__main__":
    cleanup_progress()
