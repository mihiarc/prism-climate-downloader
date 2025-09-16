#!/usr/bin/env python3
"""
Example script to download PRISM monthly climate data for 1971
This demonstrates the simplest way to get started with PRISM data downloads
"""

import os
import urllib.request
from pathlib import Path
import time

def download_prism_1971():
    """
    Download all available PRISM monthly climate variables for 1971
    """

    # Create output directory
    output_dir = Path("./prism_1971_data")
    output_dir.mkdir(exist_ok=True)

    # Base URL for PRISM web services
    base_url = "https://services.nacse.org/prism/data/public/4km"

    # Variables available from 1971 (monthly data)
    variables = {
        'ppt': 'Precipitation',
        'tmin': 'Minimum Temperature',
        'tmax': 'Maximum Temperature',
        'tmean': 'Mean Temperature',
        'tdmean': 'Dew Point Temperature',
        'vpdmin': 'Min Vapor Pressure Deficit',
        'vpdmax': 'Max Vapor Pressure Deficit'
    }

    # Months to download
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    print("="*60)
    print("PRISM Data Download for 1971")
    print("="*60)

    successful = []
    failed = []

    for var, var_name in variables.items():
        print(f"\nDownloading {var_name} ({var}) data for 1971...")

        # Create variable directory
        var_dir = output_dir / var
        var_dir.mkdir(exist_ok=True)

        for month in months:
            # Construct date string and URL
            date_str = f"1971{month}"
            url = f"{base_url}/{var}/{date_str}"
            filename = f"PRISM_{var}_4km_monthly_{date_str}.zip"
            filepath = var_dir / filename

            # Skip if already downloaded
            if filepath.exists():
                print(f"  ✓ {filename} (already exists)")
                successful.append(filename)
                continue

            # Download file
            try:
                print(f"  ⬇ Downloading {filename}...")
                urllib.request.urlretrieve(url, filepath)
                print(f"  ✓ {filename} downloaded successfully")
                successful.append(filename)

                # Small delay to be respectful of server
                time.sleep(0.5)

            except Exception as e:
                print(f"  ✗ Failed to download {filename}: {e}")
                failed.append(filename)

    # Print summary
    print("\n" + "="*60)
    print("Download Summary")
    print("="*60)
    print(f"Successfully downloaded: {len(successful)} files")
    print(f"Failed downloads: {len(failed)} files")

    if failed:
        print("\nFailed files:")
        for f in failed:
            print(f"  - {f}")

    print(f"\nData saved to: {output_dir.absolute()}")
    print("\nFile structure:")
    print("  prism_1971_data/")
    for var in variables:
        print(f"    {var}/")
        print(f"      PRISM_{var}_4km_monthly_1971*.zip")

    print("\n" + "="*60)
    print("Next Steps:")
    print("="*60)
    print("1. Extract the .zip files to access the BIL raster data")
    print("2. Use GIS software (QGIS, ArcGIS) or GDAL to read BIL files")
    print("3. Process data using Python (rasterio, GDAL) or R")
    print("\nFor more years, use the comprehensive prism_bulk_download.py script")


if __name__ == "__main__":
    download_prism_1971()