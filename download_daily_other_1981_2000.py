#!/usr/bin/env python3
"""
PRISM Daily Non-Temperature Data Downloader (1981-2000)
Downloads precipitation and humidity variables: ppt, tdmean, vpdmin, vpdmax
"""

import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from pathlib import Path
import concurrent.futures

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("⚠️  tqdm not installed. Install with: pip install tqdm")
    print("   Continuing without progress bars...\n")

class PRISMDailyOtherDownloader:
    def __init__(self, output_dir="./prism_daily_other_data"):
        self.output_dir = Path(output_dir)
        self.base_url = "https://services.nacse.org/prism/data/public/4km"
        self.ftp_base = "ftp://prism.oregonstate.edu/daily"

        # Non-temperature variables
        # ppt = precipitation, tdmean = mean dew point temp,
        # vpdmin/vpdmax = min/max vapor pressure deficit
        self.variables = ['ppt', 'tdmean', 'vpdmin', 'vpdmax']

        # Create output directories
        for var in self.variables:
            (self.output_dir / var).mkdir(parents=True, exist_ok=True)

    def generate_date_range(self, start_date, end_date):
        """Generate all dates in range"""
        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    def build_url(self, variable, date, use_ftp=False):
        """Build download URL for a specific date and variable"""
        date_str = date.strftime("%Y%m%d")
        year = date.year

        if use_ftp:
            # FTP URL structure
            url = f"{self.ftp_base}/{variable}/{year}/PRISM_{variable}_stable_4kmD2_{date_str}_bil.zip"
        else:
            # Web services URL
            url = f"{self.base_url}/{variable}/{date_str}"

        return url

    def download_file(self, url, output_path, max_retries=3):
        """Download a single file with retry logic"""
        for attempt in range(max_retries):
            try:
                # Check if file already exists
                if output_path.exists():
                    file_size = output_path.stat().st_size
                    if file_size > 1000:  # Skip if file is larger than 1KB
                        return True, "Already downloaded"

                # Download with timeout
                with urllib.request.urlopen(url, timeout=30) as response:
                    data = response.read()

                    # Save to file
                    with open(output_path, 'wb') as f:
                        f.write(data)

                    return True, "Success"

            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return False, f"File not found (404)"
                elif attempt == max_retries - 1:
                    return False, f"HTTP Error {e.code}"
                time.sleep(2 ** attempt)  # Exponential backoff

            except Exception as e:
                if attempt == max_retries - 1:
                    return False, str(e)
                time.sleep(2 ** attempt)

        return False, "Max retries exceeded"

    def download_date_variable(self, date, variable):
        """Download data for a specific date and variable"""
        date_str = date.strftime("%Y%m%d")
        filename = f"PRISM_{variable}_stable_4kmD2_{date_str}_bil.zip"
        output_path = self.output_dir / variable / filename

        # Try web services first, then FTP
        url = self.build_url(variable, date, use_ftp=False)
        success, message = self.download_file(url, output_path)

        if not success and "404" in message:
            # Try FTP as fallback
            url = self.build_url(variable, date, use_ftp=True)
            success, message = self.download_file(url, output_path)

        return {
            'date': date_str,
            'variable': variable,
            'success': success,
            'message': message,
            'path': str(output_path) if success else None
        }

    def download_range(self, start_date, end_date, variables=None, max_workers=4):
        """Download data for date range with parallel processing"""
        # Use specified variables or default to all
        vars_to_download = variables if variables else self.variables

        dates = self.generate_date_range(start_date, end_date)
        total_downloads = len(dates) * len(vars_to_download)

        print(f"\n💧 PRISM Daily Precipitation & Humidity Download")
        print(f"📅 Period: {start_date.date()} to {end_date.date()}")
        print(f"📊 Variables:")
        for var in vars_to_download:
            var_desc = {
                'ppt': 'Precipitation (mm)',
                'tdmean': 'Mean dew point temperature (°C)',
                'vpdmin': 'Minimum vapor pressure deficit (hPa)',
                'vpdmax': 'Maximum vapor pressure deficit (hPa)'
            }
            print(f"   • {var}: {var_desc.get(var, var)}")
        print(f"📁 Output: {self.output_dir}")
        print(f"🔢 Total files: {total_downloads}")
        print(f"💾 Estimated size: ~{total_downloads * 2:.0f} MB compressed")
        print("-" * 50)

        # Create download tasks
        tasks = []
        for date in dates:
            for variable in vars_to_download:
                tasks.append((date, variable))

        # Download with progress bar
        results = []
        failed = []
        skipped = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self.download_date_variable, date, var): (date, var)
                for date, var in tasks
            }

            # Process results with progress bar
            if HAS_TQDM:
                with tqdm(total=total_downloads, desc="Downloading", unit="file") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        results.append(result)

                        if result['success']:
                            if "Already" in result['message']:
                                skipped.append(result)
                        else:
                            failed.append(result)

                        # Update progress bar
                        pbar.update(1)
                        if not result['success']:
                            pbar.set_postfix({'failed': len(failed)})
            else:
                # No progress bar version
                completed = 0
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    results.append(result)
                    completed += 1

                    if result['success']:
                        if "Already" in result['message']:
                            skipped.append(result)
                    else:
                        failed.append(result)

                    # Print simple progress
                    if completed % 10 == 0 or completed == total_downloads:
                        print(f"Progress: {completed}/{total_downloads} files ({completed*100/total_downloads:.1f}%)")

        # Print summary
        print("\n" + "=" * 50)
        print("📊 Download Summary:")
        print(f"✅ Successfully downloaded: {len([r for r in results if r['success'] and 'Already' not in r['message']])}")
        print(f"⏭️  Skipped (already exists): {len(skipped)}")
        print(f"❌ Failed: {len(failed)}")

        if failed:
            print("\n⚠️  Failed downloads:")
            for f in failed[:10]:  # Show first 10 failures
                print(f"  - {f['date']} {f['variable']}: {f['message']}")
            if len(failed) > 10:
                print(f"  ... and {len(failed)-10} more")

        # Save log
        log_file = self.output_dir / "download_log.txt"
        with open(log_file, 'w') as f:
            f.write(f"PRISM Daily Precipitation & Humidity Download Log\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Period: {start_date.date()} to {end_date.date()}\n")
            f.write(f"Variables: {', '.join(vars_to_download)}\n")
            f.write(f"Total files: {total_downloads}\n")
            f.write(f"Success: {len([r for r in results if r['success']])}\n")
            f.write(f"Failed: {len(failed)}\n\n")

            if failed:
                f.write("Failed downloads:\n")
                for fail in failed:
                    f.write(f"{fail['date']},{fail['variable']},{fail['message']}\n")

        print(f"\n📝 Log saved to: {log_file}")
        return results


def main():
    # Initialize downloader
    downloader = PRISMDailyOtherDownloader(output_dir="./prism_daily_other_1981_2000")

    # Define date range
    start_date = datetime(1981, 1, 1)
    end_date = datetime(2000, 12, 31)

    # Calculate total days and estimate time
    total_days = (end_date - start_date).days + 1
    total_files = total_days * 4  # 4 variables
    estimated_hours = total_files / 3600  # Rough estimate

    print("\n" + "=" * 60)
    print("💧 PRISM DAILY PRECIPITATION & HUMIDITY DOWNLOADER (1981-2000)")
    print("=" * 60)
    print(f"\n📊 Download Details:")
    print(f"   • Period: {total_days:,} days ({total_days/365:.1f} years)")
    print(f"   • Variables:")
    print(f"     - ppt: Precipitation (mm)")
    print(f"     - tdmean: Mean dew point temperature (°C)")
    print(f"     - vpdmin: Minimum vapor pressure deficit (hPa)")
    print(f"     - vpdmax: Maximum vapor pressure deficit (hPa)")
    print(f"   • Total files: {total_files:,}")
    print(f"   • Estimated download time: {estimated_hours:.1f} hours")
    print(f"   • Estimated storage: ~{total_files * 2 / 1000:.1f} GB (compressed)")

    # Option to select specific variables
    print("\n📋 Variable Selection:")
    print("   1. Download all variables (ppt, tdmean, vpdmin, vpdmax)")
    print("   2. Download only precipitation (ppt)")
    print("   3. Download only humidity variables (tdmean, vpdmin, vpdmax)")
    print("   4. Custom selection")

    choice = input("\nSelect option (1-4): ")

    variables_to_download = None
    if choice == '2':
        variables_to_download = ['ppt']
        print("✅ Selected: Precipitation only")
    elif choice == '3':
        variables_to_download = ['tdmean', 'vpdmin', 'vpdmax']
        print("✅ Selected: Humidity variables only")
    elif choice == '4':
        print("\nAvailable variables: ppt, tdmean, vpdmin, vpdmax")
        custom = input("Enter variables separated by spaces: ").strip().split()
        valid_vars = [v for v in custom if v in ['ppt', 'tdmean', 'vpdmin', 'vpdmax']]
        if valid_vars:
            variables_to_download = valid_vars
            print(f"✅ Selected: {', '.join(valid_vars)}")
        else:
            print("❌ No valid variables selected. Using all variables.")
    else:
        print("✅ Selected: All variables")

    # Update file count if custom selection
    if variables_to_download:
        total_files = total_days * len(variables_to_download)
        print(f"\n📊 Updated: {total_files:,} files to download")

    # Confirm before starting large download
    print("\n⚠️  This is a large download that may take several hours!")
    print("💡 Tip: You can interrupt and resume later (already downloaded files will be skipped)")

    response = input("\n🚀 Start download? (y/n): ")
    if response.lower() != 'y':
        print("❌ Download cancelled")
        return

    # Start download
    print("\n🔄 Starting download with 4 parallel connections...")
    results = downloader.download_range(start_date, end_date,
                                       variables=variables_to_download,
                                       max_workers=4)

    print("\n✅ Download complete!")
    print(f"📁 Data saved to: ./prism_daily_other_1981_2000/")


if __name__ == "__main__":
    main()