#!/usr/bin/env python3
"""
PRISM Daily All Variables Downloader (2001-2024)
Downloads all available daily climate variables: tmin, tmax, tmean, ppt, tdmean, vpdmin, vpdmax
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

class PRISMDailyAllVariablesDownloader:
    def __init__(self, output_dir="./prism_daily_data"):
        self.output_dir = Path(output_dir)
        self.base_url = "https://services.nacse.org/prism/data/public/4km"
        self.ftp_base = "ftp://prism.oregonstate.edu/daily"

        # All available daily variables
        self.variables = {
            # Temperature variables
            'tmin': 'Minimum temperature (°C)',
            'tmax': 'Maximum temperature (°C)',
            'tmean': 'Mean temperature (°C)',
            # Precipitation
            'ppt': 'Precipitation (mm)',
            # Humidity variables
            'tdmean': 'Mean dew point temperature (°C)',
            'vpdmin': 'Minimum vapor pressure deficit (hPa)',
            'vpdmax': 'Maximum vapor pressure deficit (hPa)'
        }

        # Create output directories
        for var in self.variables.keys():
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
        vars_to_download = variables if variables else list(self.variables.keys())

        dates = self.generate_date_range(start_date, end_date)
        total_downloads = len(dates) * len(vars_to_download)

        print(f"\n🌍 PRISM Daily All Variables Download")
        print(f"📅 Period: {start_date.date()} to {end_date.date()} ({len(dates)} days)")
        print(f"📊 Variables ({len(vars_to_download)}):")
        for var in vars_to_download:
            print(f"   • {var}: {self.variables[var]}")
        print(f"📁 Output: {self.output_dir}")
        print(f"🔢 Total files: {total_downloads:,}")
        print(f"💾 Estimated size: ~{total_downloads * 2:.0f} MB compressed")
        print("-" * 60)

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
                    if completed % 50 == 0 or completed == total_downloads:
                        print(f"Progress: {completed:,}/{total_downloads:,} files ({completed*100/total_downloads:.1f}%)")

        # Print summary
        print("\n" + "=" * 60)
        print("📊 Download Summary:")
        print(f"✅ Successfully downloaded: {len([r for r in results if r['success'] and 'Already' not in r['message']]):,}")
        print(f"⏭️  Skipped (already exists): {len(skipped):,}")
        print(f"❌ Failed: {len(failed):,}")

        if failed:
            print("\n⚠️  Failed downloads:")
            # Group failures by date for better readability
            failed_by_date = {}
            for f in failed:
                date = f['date']
                if date not in failed_by_date:
                    failed_by_date[date] = []
                failed_by_date[date].append(f['variable'])

            dates_shown = 0
            for date, vars in sorted(failed_by_date.items())[:5]:
                print(f"  - {date}: {', '.join(vars)}")
                dates_shown += 1

            if len(failed_by_date) > 5:
                print(f"  ... and {len(failed_by_date)-5} more dates with failures")

        # Save detailed log
        log_file = self.output_dir / "download_log.txt"
        with open(log_file, 'w') as f:
            f.write(f"PRISM Daily All Variables Download Log\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Period: {start_date.date()} to {end_date.date()}\n")
            f.write(f"Days: {len(dates)}\n")
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
    downloader = PRISMDailyAllVariablesDownloader(output_dir="./prism_daily_all_2001_2024")

    # Define date range for 2001-2024
    start_date = datetime(2001, 1, 1)
    # Use current date or end of 2024, whichever is earlier
    current_date = datetime.now()
    end_of_2024 = datetime(2024, 12, 31)
    end_date = min(current_date - timedelta(days=2), end_of_2024)  # 2-day lag for data availability

    # Calculate total days and estimate
    total_days = (end_date - start_date).days + 1
    total_files = total_days * 7  # 7 variables

    print("\n" + "=" * 70)
    print("🌍 PRISM DAILY ALL VARIABLES DOWNLOADER (2001-2024)")
    print("=" * 70)
    print(f"\n📊 Download Details:")
    print(f"   • Period: {start_date.date()} to {end_date.date()}")
    print(f"   • Total days: {total_days:,} ({total_days/365:.1f} years)")
    print(f"   • Variables (7 total):")
    print(f"     Temperature: tmin, tmax, tmean")
    print(f"     Precipitation: ppt")
    print(f"     Humidity: tdmean, vpdmin, vpdmax")
    print(f"   • Total files: {total_files:,}")
    print(f"   • Estimated download time: {total_files/3600:.1f} hours")
    print(f"   • Estimated storage: ~{total_files * 2 / 1000:.1f} GB (compressed)")

    # Options for download
    print("\n📋 Download Options:")
    print("   1. Download ALL variables (7 variables)")
    print("   2. Temperature only (tmin, tmax, tmean)")
    print("   3. Precipitation only (ppt)")
    print("   4. Humidity only (tdmean, vpdmin, vpdmax)")
    print("   5. Custom year range")
    print("   6. Test mode (download just 3 days)")

    choice = input("\nSelect option (1-6): ")

    variables_to_download = None
    custom_start = start_date
    custom_end = end_date

    if choice == '2':
        variables_to_download = ['tmin', 'tmax', 'tmean']
        print("✅ Selected: Temperature variables only")
    elif choice == '3':
        variables_to_download = ['ppt']
        print("✅ Selected: Precipitation only")
    elif choice == '4':
        variables_to_download = ['tdmean', 'vpdmin', 'vpdmax']
        print("✅ Selected: Humidity variables only")
    elif choice == '5':
        print("\nCustom year range (2001-2024):")
        start_year = input("Enter start year (2001-2024): ")
        end_year = input("Enter end year (2001-2024): ")
        try:
            start_year = max(2001, min(2024, int(start_year)))
            end_year = max(2001, min(2024, int(end_year)))
            custom_start = datetime(start_year, 1, 1)
            custom_end = datetime(end_year, 12, 31)
            custom_end = min(custom_end, end_date)  # Don't exceed data availability
            print(f"✅ Selected period: {custom_start.date()} to {custom_end.date()}")
        except:
            print("❌ Invalid year. Using full range.")
    elif choice == '6':
        # Test mode - just 3 days
        custom_end = custom_start + timedelta(days=2)
        print(f"✅ TEST MODE: Downloading 3 days ({custom_start.date()} to {custom_end.date()})")
    else:
        print("✅ Selected: All variables")

    # Update file count if custom selection
    if variables_to_download or custom_start != start_date or custom_end != end_date:
        days = (custom_end - custom_start).days + 1
        vars_count = len(variables_to_download) if variables_to_download else 7
        total_files = days * vars_count
        print(f"\n📊 Updated: {total_files:,} files to download")
        print(f"   • {days:,} days × {vars_count} variables")

    # Worker count selection
    print("\n⚙️  Download speed (parallel connections):")
    print("   1. Conservative (2 workers) - Slowest, most reliable")
    print("   2. Standard (4 workers) - Recommended")
    print("   3. Fast (6 workers) - Faster, may have more failures")
    print("   4. Aggressive (8 workers) - Fastest, highest failure risk")

    worker_choice = input("\nSelect speed (1-4) [default: 2]: ").strip() or '2'
    worker_map = {'1': 2, '2': 4, '3': 6, '4': 8}
    max_workers = worker_map.get(worker_choice, 4)
    print(f"✅ Using {max_workers} parallel connections")

    # Confirm before starting large download
    if total_files > 100:
        print(f"\n⚠️  This is a large download ({total_files:,} files) that may take several hours!")
    print("💡 Tip: You can interrupt and resume later (already downloaded files will be skipped)")

    response = input("\n🚀 Start download? (y/n): ")
    if response.lower() != 'y':
        print("❌ Download cancelled")
        return

    # Start download
    print(f"\n🔄 Starting download with {max_workers} parallel connections...")
    results = downloader.download_range(custom_start, custom_end,
                                       variables=variables_to_download,
                                       max_workers=max_workers)

    # Final summary
    successful = len([r for r in results if r['success']])
    failed = len([r for r in results if not r['success']])

    print("\n" + "=" * 70)
    print("✅ DOWNLOAD COMPLETE!")
    print(f"📁 Data saved to: {downloader.output_dir}")
    print(f"📊 Final results: {successful:,} successful, {failed:,} failed")

    if failed > 0:
        print(f"\n💡 To retry failed downloads, simply run this script again.")
        print(f"   It will skip existing files and only download missing ones.")


if __name__ == "__main__":
    main()