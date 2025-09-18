#!/usr/bin/env python3
"""
Convert all PRISM temperature data (1981-2000) to Zarr stores
"""

from pathlib import Path
from datetime import datetime
import logging
import sys
from prism_to_zarr import PRISMToZarrConverter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Convert all temperature variables to Zarr format
    """
    print("="*70)
    print("PRISM Temperature Data to Zarr Conversion (1981-2000)")
    print("="*70)

    # Paths
    input_base = Path("/home/mihiarc/prism/prism_daily_temp_1981_2000")
    output_base = Path("/home/mihiarc/prism/zarr_stores")

    # Create output directory
    output_base.mkdir(parents=True, exist_ok=True)

    # Date range for 1981-2000
    start_date = datetime(1981, 1, 1)
    end_date = datetime(2000, 12, 31)

    # Initialize converter with time-optimized chunking
    converter = PRISMToZarrConverter(resolution='4km', chunk_strategy='time_optimized')

    # Variables to process
    variables = ['tmin', 'tmax', 'tmean']

    # Check if we should clean existing stores
    for variable in variables:
        output_zarr = output_base / f"{variable}_1981_2000.zarr"
        if output_zarr.exists():
            logger.warning(f"Removing existing Zarr store: {output_zarr}")
            import shutil
            shutil.rmtree(output_zarr)

    for variable in variables:
        var_dir = input_base / variable

        if not var_dir.exists():
            logger.warning(f"Directory {var_dir} does not exist, skipping {variable}")
            continue

        # Count available files
        zip_files = list(var_dir.glob("*.zip"))
        logger.info(f"Found {len(zip_files)} files for {variable}")

        if not zip_files:
            logger.warning(f"No zip files found for {variable}, skipping")
            continue

        # Output zarr path
        output_zarr = output_base / f"{variable}_1981_2000.zarr"

        print(f"\n{'='*70}")
        print(f"Processing {variable.upper()}")
        print(f"Input: {var_dir}")
        print(f"Output: {output_zarr}")
        print(f"{'='*70}")

        try:
            # Process the entire time series
            converter.process_time_series(
                var_dir,
                variable,
                start_date,
                end_date,
                output_zarr,
                batch_days=365  # Process one year at a time
            )

            # Get info about the created store
            if output_zarr.exists():
                info = converter.get_zarr_info(output_zarr)
                print(f"\n✓ Successfully created {variable} Zarr store:")
                print(f"  - Dimensions: {info['dimensions']}")
                print(f"  - Time range: {info['time_range'][0]} to {info['time_range'][1]}")
                print(f"  - Size: {info['size_mb']:.2f} MB")
                print(f"  - Chunks: {info['chunks'].get(variable, 'N/A')}")

        except Exception as e:
            logger.error(f"Error processing {variable}: {e}")
            continue

    print(f"\n{'='*70}")
    print("Conversion complete!")
    print(f"Zarr stores created in: {output_base}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()