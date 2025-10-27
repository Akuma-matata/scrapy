
"""
GMrepo Data Downloader
Downloads species abundance data for runs with QCStatus=1 from GMrepo database.

Features:
- Read run IDs from TSV file or scrape from API
- Parallel downloads with rate limiting
- Progress tracking
- Resume capability
- Error logging
- Skip existing files
"""

import requests
import pandas as pd
import json
import time
import os
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import logging
from datetime import datetime

class GMrepoDownloader:
    def __init__(self, output_dir, max_workers=5, delay=0.5):
        """
        Initialize the downloader.
        
        Args:
            output_dir: Directory to save CSV files
            max_workers: Number of parallel download threads
            delay: Delay between requests in seconds
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_workers = max_workers
        self.delay = delay
        
        self.base_url = "https://gmrepo.humangut.info"
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Thread-safe counters
        self.lock = Lock()
        self.downloaded_count = 0
        self.failed_count = 0
        
        # Setup logging
        self.setup_logging()
        
        # Files for tracking progress
        self.progress_file = self.output_dir / "progress.json"
        self.failed_log = self.output_dir / "failed_downloads.txt"
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.output_dir / f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_run_ids_from_tsv(self, tsv_file):
        """
        Read runIDs with QCStatus=1 from TSV file.

        Args:
            tsv_file: Path to the TSV file containing run information

        Returns:
            List of runIDs with QCStatus=1
        """
        self.logger.info(f"Reading runIDs from TSV file: {tsv_file}")

        try:
            # Read TSV file
            df = pd.read_csv(tsv_file, sep='\t')

            self.logger.info(f"Total runs in TSV: {len(df)}")

            # Filter for QCstatus == 1 or 1.0
            df_filtered = df[df['QCstatus'] == 1.0]

            # Extract run_ids
            run_ids = df_filtered['run_id'].tolist()

            self.logger.info(f"Found {len(run_ids)} runIDs with QCStatus=1")
            return run_ids

        except Exception as e:
            self.logger.error(f"Error reading TSV file: {str(e)}")
            raise

    def get_all_run_ids(self, mesh_id="D006262", total_pages=4959):
        """
        Fetch all runIDs with QCStatus=1 from the API.

        Args:
            mesh_id: Phenotype mesh ID
            total_pages: Total number of pages to fetch

        Returns:
            List of runIDs with QCStatus=1
        """
        self.logger.info(f"Fetching runIDs from {total_pages} pages...")
        run_ids = []
        limit = 10

        with tqdm(total=total_pages, desc="Fetching runIDs", unit="page") as pbar:
            for page in range(total_pages):
                skip = page * limit

                try:
                    response = self.session.post(
                        f"{self.base_url}/api/getAssociatedRunsByPhenotypeMeshIDLimit/",
                        json={
                            "mesh_id": mesh_id,
                            "limit": limit,
                            "skip": skip
                        },
                        timeout=30
                    )
                    response.raise_for_status()

                    data = response.json()

                    # Filter for QCStatus=1
                    for run in data:
                        if run.get("QCStatus") == 1:
                            run_ids.append(run["run_id"])

                    pbar.update(1)
                    time.sleep(self.delay)  # Be polite to the server

                except Exception as e:
                    self.logger.error(f"Error fetching page {page}: {str(e)}")
                    continue

        self.logger.info(f"Found {len(run_ids)} runIDs with QCStatus=1")
        return run_ids
    
    def get_loaded_uid(self, run_id):
        """
        Get the loaded_uid for a specific runID.
        
        Args:
            run_id: The run identifier
            
        Returns:
            loaded_uid or None if failed
        """
        try:
            response = self.session.post(
                f"{self.base_url}/api/getRunDetailsByRunID/",
                json={"run_id": run_id},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            return data["run"]["loaded_uid"]
        except Exception as e:
            self.logger.error(f"Error getting loaded_uid for {run_id}: {str(e)}")
            return None
    
    def get_abundance_data(self, loaded_uid):
        """
        Get relative abundance data.
        
        Args:
            loaded_uid: The loaded UID
            
        Returns:
            JSON data or None if failed
        """
        try:
            response = self.session.post(
                f"{self.base_url}/api/getRelativeAbundanceByRunID/",
                json={
                    "loaded_uid": loaded_uid,
                    "taxon_level": "species"
                },
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Error getting abundance data for loaded_uid {loaded_uid}: {str(e)}")
            return None
    
    def download_single_run(self, run_id):
        """
        Download data for a single runID and save as CSV.
        
        Args:
            run_id: The run identifier
            
        Returns:
            Tuple of (success, run_id, error_message)
        """
        output_file = self.output_dir / f"{run_id}.csv"
        
        # Skip if file already exists
        if output_file.exists():
            return (True, run_id, "Already exists")
        
        try:
            # Step 1: Get loaded_uid
            loaded_uid = self.get_loaded_uid(run_id)
            if loaded_uid is None:
                return (False, run_id, "Failed to get loaded_uid")
            
            time.sleep(self.delay)  # Rate limiting
            
            # Step 2: Get abundance data
            abundance_data = self.get_abundance_data(loaded_uid)
            if abundance_data is None or len(abundance_data) == 0:
                return (False, run_id, "Failed to get abundance data")
            
            # Step 3: Convert to DataFrame and save as CSV
            df = pd.DataFrame(abundance_data)
            df.to_csv(output_file, index=False)
            
            with self.lock:
                self.downloaded_count += 1
            
            return (True, run_id, None)
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error downloading {run_id}: {error_msg}")
            with self.lock:
                self.failed_count += 1
            return (False, run_id, error_msg)
    
    def save_failed_download(self, run_id, error_msg):
        """Save failed download to log file"""
        with open(self.failed_log, 'a') as f:
            f.write(f"{run_id}\t{error_msg}\t{datetime.now().isoformat()}\n")
    
    def save_progress(self, completed_runs):
        """Save progress to file for resume capability"""
        progress_data = {
            "completed": list(completed_runs),
            "downloaded_count": self.downloaded_count,
            "failed_count": self.failed_count,
            "timestamp": datetime.now().isoformat()
        }
        with open(self.progress_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
    
    def load_progress(self):
        """Load previous progress if exists"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.logger.info(f"Resuming from previous session. {len(data['completed'])} runs already completed.")
                    return set(data['completed'])
            except Exception as e:
                self.logger.error(f"Error loading progress file: {str(e)}")
        return set()
    
    def download_all(self, run_ids):
        """
        Download data for all runIDs with parallel execution.
        
        Args:
            run_ids: List of run identifiers
        """
        # Load previous progress
        completed_runs = self.load_progress()
        
        # Filter out already completed runs
        remaining_runs = [rid for rid in run_ids if rid not in completed_runs]
        
        if not remaining_runs:
            self.logger.info("All runs already downloaded!")
            return
        
        self.logger.info(f"Starting download of {len(remaining_runs)} runs...")
        self.logger.info(f"Using {self.max_workers} parallel workers with {self.delay}s delay")
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_run = {executor.submit(self.download_single_run, run_id): run_id 
                            for run_id in remaining_runs}
            
            # Process completed tasks with progress bar
            with tqdm(total=len(remaining_runs), desc="Downloading", unit="file") as pbar:
                for future in as_completed(future_to_run):
                    success, run_id, error_msg = future.result()
                    
                    if success:
                        if error_msg != "Already exists":
                            self.logger.debug(f"Successfully downloaded {run_id}")
                    else:
                        self.logger.warning(f"Failed to download {run_id}: {error_msg}")
                        self.save_failed_download(run_id, error_msg)
                    
                    completed_runs.add(run_id)
                    pbar.update(1)
                    
                    # Save progress every 50 downloads
                    if len(completed_runs) % 50 == 0:
                        self.save_progress(completed_runs)
        
        # Final progress save
        self.save_progress(completed_runs)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print download summary"""
        self.logger.info("\n" + "="*60)
        self.logger.info("DOWNLOAD SUMMARY")
        self.logger.info("="*60)
        self.logger.info(f"Successfully downloaded: {self.downloaded_count}")
        self.logger.info(f"Failed downloads: {self.failed_count}")
        self.logger.info(f"Output directory: {self.output_dir.absolute()}")
        
        if self.failed_count > 0:
            self.logger.info(f"Failed downloads logged to: {self.failed_log}")
        
        self.logger.info("="*60)


def main():
    parser = argparse.ArgumentParser(
        description='Download species abundance data from GMrepo for healthy phenotype (QCStatus=1)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download using TSV file (recommended - skips scraping)
  python gmrepo_downloader.py --tsv-file all_runs_associated_with_D006262.tsv

  # Download to default directory (Healthy_Data) by scraping API
  python gmrepo_downloader.py

  # Specify custom output directory with TSV file
  python gmrepo_downloader.py --tsv-file all_runs_associated_with_D006262.tsv --output-dir /path/to/my/data

  # Adjust parallel workers and delay
  python gmrepo_downloader.py --tsv-file all_runs_associated_with_D006262.tsv --workers 10 --delay 0.3

  # Resume from interruption (automatically detects previous progress)
  python gmrepo_downloader.py --tsv-file all_runs_associated_with_D006262.tsv
        """
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='Healthy_Data',
        help='Directory to save CSV files (default: Healthy_Data)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Number of parallel download threads (default: 5)'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help='Delay between requests in seconds (default: 0.5)'
    )
    
    parser.add_argument(
        '--mesh-id',
        type=str,
        default='D006262',
        help='Phenotype mesh ID (default: D006262 for Health)'
    )
    
    parser.add_argument(
        '--pages',
        type=int,
        default=4959,
        help='Total pages to fetch (default: 4959)'
    )

    parser.add_argument(
        '--tsv-file',
        type=str,
        default=None,
        help='TSV file containing run IDs (if provided, will use this instead of scraping)'
    )

    args = parser.parse_args()

    print("="*60)
    print("GMrepo Data Downloader")
    print("="*60)
    print(f"Output directory: {args.output_dir}")
    print(f"Parallel workers: {args.workers}")
    print(f"Request delay: {args.delay}s")
    if args.tsv_file:
        print(f"TSV file: {args.tsv_file}")
    else:
        print(f"Mesh ID: {args.mesh_id}")
    print("="*60)
    print()

    # Initialize downloader
    downloader = GMrepoDownloader(
        output_dir=args.output_dir,
        max_workers=args.workers,
        delay=args.delay
    )

    # Get all run IDs with QCStatus=1
    if args.tsv_file:
        # Read run IDs from TSV file
        run_ids = downloader.get_run_ids_from_tsv(args.tsv_file)
    else:
        # Scrape run IDs from API
        run_ids = downloader.get_all_run_ids(mesh_id=args.mesh_id, total_pages=args.pages)

    if not run_ids:
        print("No runIDs found with QCStatus=1")
        return

    # Download all data
    downloader.download_all(run_ids)
    
    print("\nDownload complete!")


if __name__ == "__main__":
    main()