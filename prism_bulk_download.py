#!/usr/bin/env python3
"""
PRISM Climate Data Bulk Download Script
Downloads PRISM climate data from 1971 to present
Author: Data Engineering Assistant
Date: 2025-09-16
"""

import os
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('prism_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PRISMDownloader:
    """
    Class to handle bulk downloads of PRISM climate data
    """

    # Base URLs for different data access methods
    BASE_URL_WEB = "https://services.nacse.org/prism/data/public/4km"
    FTP_SERVER = "prism.oregonstate.edu"

    # Available climate variables
    VARIABLES = {
        'ppt': 'Precipitation',
        'tmin': 'Minimum Temperature',
        'tmax': 'Maximum Temperature',
        'tmean': 'Mean Temperature',
        'tdmean': 'Mean Dew Point Temperature',
        'vpdmin': 'Minimum Vapor Pressure Deficit',
        'vpdmax': 'Maximum Vapor Pressure Deficit'
    }

    # Data availability by temporal resolution
    TEMPORAL_AVAILABILITY = {
        'daily': {
            'start': datetime(1981, 1, 1),
            'variables': ['ppt', 'tmin', 'tmax', 'tmean', 'tdmean', 'vpdmin', 'vpdmax']
        },
        'monthly': {
            'start': datetime(1895, 1, 1),
            'variables': ['ppt', 'tmin', 'tmax', 'tmean', 'tdmean', 'vpdmin', 'vpdmax']
        },
        'annual': {
            'start': datetime(1895, 1, 1),
            'variables': ['ppt', 'tmin', 'tmax', 'tmean']
        }
    }

    def __init__(self, output_dir, temporal='monthly', resolution='4km'):
        """
        Initialize PRISM Downloader

        Parameters:
        -----------
        output_dir : str
            Directory to save downloaded files
        temporal : str
            Temporal resolution ('daily', 'monthly', 'annual')
        resolution : str
            Spatial resolution ('4km' or '800m')
        """
        self.output_dir = Path(output_dir)
        self.temporal = temporal
        self.resolution = resolution

        # Create output directory structure
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Initialized PRISM Downloader")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Temporal resolution: {temporal}")
        logger.info(f"Spatial resolution: {resolution}")

    def _progress_hook(self, block_num, block_size, total_size, pbar):
        """Progress hook for urllib downloads"""
        downloaded = block_num * block_size
        if total_size > 0:
            pbar.total = total_size
            pbar.update(min(block_size, total_size - pbar.n))

    def download_file(self, url, output_path, retries=3):
        """
        Download a single file with retry logic

        Parameters:
        -----------
        url : str
            URL to download from
        output_path : Path
            Path to save the file
        retries : int
            Number of retry attempts
        """
        for attempt in range(retries):
            try:
                # Create progress bar
                with tqdm(unit='B', unit_scale=True, unit_divisor=1024,
                         miniters=1, desc=f'Downloading {output_path.name}') as pbar:

                    # Download file
                    urllib.request.urlretrieve(
                        url,
                        output_path,
                        reporthook=lambda b, bs, ts: self._progress_hook(b, bs, ts, pbar)
                    )

                logger.info(f"Successfully downloaded: {output_path.name}")
                return True

            except urllib.error.URLError as e:
                logger.warning(f"Download failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to download after {retries} attempts: {url}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error downloading {url}: {e}")
                return False

    def extract_zip(self, zip_path, extract_to=None):
        """
        Extract a zip file

        Parameters:
        -----------
        zip_path : Path
            Path to the zip file
        extract_to : Path
            Directory to extract to (default: same as zip file)
        """
        if extract_to is None:
            extract_to = zip_path.parent

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logger.info(f"Extracted: {zip_path.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to extract {zip_path}: {e}")
            return False

    def generate_daily_urls(self, variable, start_date, end_date):
        """
        Generate URLs for daily data downloads

        Parameters:
        -----------
        variable : str
            Climate variable to download
        start_date : datetime
            Start date
        end_date : datetime
            End date

        Returns:
        --------
        list : List of (url, filename) tuples
        """
        urls = []
        current = start_date

        while current <= end_date:
            date_str = current.strftime("%Y%m%d")
            url = f"{self.BASE_URL_WEB}/{variable}/{date_str}"
            filename = f"PRISM_{variable}_{self.resolution}_daily_{date_str}.zip"
            urls.append((url, filename))
            current += timedelta(days=1)

        return urls

    def generate_monthly_urls(self, variable, start_date, end_date):
        """
        Generate URLs for monthly data downloads

        Parameters:
        -----------
        variable : str
            Climate variable to download
        start_date : datetime
            Start date (will be adjusted to first of month)
        end_date : datetime
            End date (will be adjusted to first of month)

        Returns:
        --------
        list : List of (url, filename) tuples
        """
        urls = []

        # Adjust to first of month
        current = start_date.replace(day=1)
        end = end_date.replace(day=1)

        while current <= end:
            date_str = current.strftime("%Y%m")
            year = current.year
            month = current.month

            # Construct URL for monthly data
            # Format: /monthly/variable/year/month
            url = f"{self.BASE_URL_WEB}/monthly/{variable}/{year}/{month:02d}"
            filename = f"PRISM_{variable}_{self.resolution}_monthly_{date_str}.zip"
            urls.append((url, filename))

            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return urls

    def generate_annual_urls(self, variable, start_year, end_year):
        """
        Generate URLs for annual data downloads

        Parameters:
        -----------
        variable : str
            Climate variable to download
        start_year : int
            Start year
        end_year : int
            End year

        Returns:
        --------
        list : List of (url, filename) tuples
        """
        urls = []

        for year in range(start_year, end_year + 1):
            url = f"{self.BASE_URL_WEB}/annual/{variable}/{year}"
            filename = f"PRISM_{variable}_{self.resolution}_annual_{year}.zip"
            urls.append((url, filename))

        return urls

    def download_batch(self, urls, max_workers=4):
        """
        Download multiple files in parallel

        Parameters:
        -----------
        urls : list
            List of (url, filename) tuples
        max_workers : int
            Maximum number of parallel downloads
        """
        # Create variable subdirectories
        downloaded = []
        failed = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for url, filename in urls:
                # Determine variable from filename
                variable = filename.split('_')[1]
                var_dir = self.output_dir / variable / self.temporal
                var_dir.mkdir(parents=True, exist_ok=True)

                output_path = var_dir / filename

                # Skip if file already exists
                if output_path.exists():
                    logger.info(f"File already exists, skipping: {filename}")
                    downloaded.append(filename)
                    continue

                # Submit download task
                future = executor.submit(self.download_file, url, output_path)
                futures[future] = (url, output_path)

            # Process completed downloads
            for future in as_completed(futures):
                url, output_path = futures[future]
                try:
                    success = future.result()
                    if success:
                        downloaded.append(output_path.name)

                        # Extract zip file if needed
                        if output_path.suffix == '.zip':
                            self.extract_zip(output_path)
                    else:
                        failed.append(output_path.name)
                except Exception as e:
                    logger.error(f"Error processing {output_path.name}: {e}")
                    failed.append(output_path.name)

        return downloaded, failed

    def download_data(self, variables, start_date, end_date, max_workers=4):
        """
        Main method to download PRISM data

        Parameters:
        -----------
        variables : list
            List of climate variables to download
        start_date : datetime or int
            Start date (datetime for daily/monthly, int for annual)
        end_date : datetime or int
            End date (datetime for daily/monthly, int for annual)
        max_workers : int
            Maximum number of parallel downloads
        """
        logger.info("="*60)
        logger.info("Starting PRISM data download")
        logger.info(f"Variables: {variables}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info("="*60)

        all_urls = []

        for variable in variables:
            logger.info(f"Generating URLs for {variable} ({self.VARIABLES.get(variable, 'Unknown')})")

            if self.temporal == 'daily':
                urls = self.generate_daily_urls(variable, start_date, end_date)
            elif self.temporal == 'monthly':
                urls = self.generate_monthly_urls(variable, start_date, end_date)
            elif self.temporal == 'annual':
                urls = self.generate_annual_urls(variable, start_date, end_date)
            else:
                logger.error(f"Invalid temporal resolution: {self.temporal}")
                continue

            all_urls.extend(urls)
            logger.info(f"Generated {len(urls)} URLs for {variable}")

        logger.info(f"Total files to download: {len(all_urls)}")

        # Download files
        downloaded, failed = self.download_batch(all_urls, max_workers)

        # Summary
        logger.info("="*60)
        logger.info("Download Summary")
        logger.info(f"Successfully downloaded: {len(downloaded)} files")
        logger.info(f"Failed downloads: {len(failed)} files")

        if failed:
            logger.warning("Failed files:")
            for f in failed:
                logger.warning(f"  - {f}")

        logger.info("="*60)

        return downloaded, failed


def main():
    """
    Main function to run the PRISM bulk downloader
    """
    parser = argparse.ArgumentParser(
        description='PRISM Climate Data Bulk Downloader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download monthly data from 1971 to 1980
  python prism_bulk_download.py --temporal monthly --start 1971-01-01 --end 1980-12-31 --variables ppt tmin tmax

  # Download daily data for 2020
  python prism_bulk_download.py --temporal daily --start 2020-01-01 --end 2020-12-31 --variables ppt

  # Download all available variables for monthly data from 1971
  python prism_bulk_download.py --temporal monthly --start 1971-01-01 --end 2023-12-31 --all-variables
        """
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./prism_data',
        help='Output directory for downloaded files (default: ./prism_data)'
    )

    parser.add_argument(
        '--temporal',
        type=str,
        choices=['daily', 'monthly', 'annual'],
        default='monthly',
        help='Temporal resolution (default: monthly)'
    )

    parser.add_argument(
        '--resolution',
        type=str,
        choices=['4km', '800m'],
        default='4km',
        help='Spatial resolution (default: 4km)'
    )

    parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD for daily/monthly, YYYY for annual)'
    )

    parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='End date (YYYY-MM-DD for daily/monthly, YYYY for annual)'
    )

    parser.add_argument(
        '--variables',
        nargs='+',
        choices=list(PRISMDownloader.VARIABLES.keys()),
        help='Climate variables to download'
    )

    parser.add_argument(
        '--all-variables',
        action='store_true',
        help='Download all available variables for the temporal resolution'
    )

    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of parallel downloads (default: 4)'
    )

    args = parser.parse_args()

    # Parse dates
    if args.temporal == 'annual':
        start_date = int(args.start)
        end_date = int(args.end)
    else:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')

    # Determine variables to download
    if args.all_variables:
        variables = PRISMDownloader.TEMPORAL_AVAILABILITY[args.temporal]['variables']
    elif args.variables:
        variables = args.variables
    else:
        logger.error("Please specify --variables or use --all-variables")
        sys.exit(1)

    # Validate date range
    if args.temporal in ['daily', 'monthly']:
        min_date = PRISMDownloader.TEMPORAL_AVAILABILITY[args.temporal]['start']
        if start_date < min_date:
            logger.warning(f"{args.temporal.capitalize()} data only available from {min_date.strftime('%Y-%m-%d')}")
            logger.warning(f"Adjusting start date to {min_date.strftime('%Y-%m-%d')}")
            start_date = min_date

    # Initialize downloader
    downloader = PRISMDownloader(
        output_dir=args.output_dir,
        temporal=args.temporal,
        resolution=args.resolution
    )

    # Start download
    downloader.download_data(
        variables=variables,
        start_date=start_date,
        end_date=end_date,
        max_workers=args.max_workers
    )


if __name__ == "__main__":
    main()