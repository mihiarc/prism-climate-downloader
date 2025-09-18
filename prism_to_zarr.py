#!/usr/bin/env python3
"""
PRISM to Zarr Converter
Converts PRISM BIL format data to efficient Zarr stores for analysis
"""

import os
import sys
import zipfile
import numpy as np
import xarray as xr
import zarr
from numcodecs import Blosc
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from tqdm import tqdm
import json
import shutil
from process_prism_data import PRISMProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PRISMToZarrConverter:
    """
    Converts PRISM BIL format data to Zarr stores
    """

    # PRISM variable units and descriptions
    VARIABLE_INFO = {
        'tmin': {'units': 'degrees_celsius', 'long_name': 'Daily Minimum Temperature'},
        'tmax': {'units': 'degrees_celsius', 'long_name': 'Daily Maximum Temperature'},
        'tmean': {'units': 'degrees_celsius', 'long_name': 'Daily Mean Temperature'},
        'ppt': {'units': 'millimeters', 'long_name': 'Daily Precipitation'},
        'tdmean': {'units': 'degrees_celsius', 'long_name': 'Daily Mean Dew Point Temperature'},
        'vpdmin': {'units': 'hectopascals', 'long_name': 'Daily Minimum Vapor Pressure Deficit'},
        'vpdmax': {'units': 'hectopascals', 'long_name': 'Daily Maximum Vapor Pressure Deficit'}
    }

    # Chunking recommendations
    CHUNK_CONFIGS = {
        'time_optimized': {'time': 365, 'lat': 621, 'lon': 1405},  # Full spatial, 1 year temporal
        'space_optimized': {'time': -1, 'lat': 155, 'lon': 351},  # All time, spatial quarters
        'balanced': {'time': 30, 'lat': 207, 'lon': 468}  # Monthly chunks, spatial thirds
    }

    def __init__(self, resolution: str = '4km', chunk_strategy: str = 'time_optimized'):
        """
        Initialize converter

        Parameters:
        -----------
        resolution : str
            PRISM data resolution ('4km' or '800m')
        chunk_strategy : str
            Chunking strategy ('time_optimized', 'space_optimized', 'balanced')
        """
        self.processor = PRISMProcessor(resolution)
        self.resolution = resolution
        self.chunk_config = self.CHUNK_CONFIGS[chunk_strategy]

        # Grid specifications from processor
        self.specs = self.processor.specs

        # Create coordinate arrays
        self.lon = np.arange(
            self.specs['xllcorner'],
            self.specs['xllcorner'] + self.specs['ncols'] * self.specs['cellsize'],
            self.specs['cellsize']
        )[:self.specs['ncols']]

        self.lat = np.arange(
            self.specs['yllcorner'],
            self.specs['yllcorner'] + self.specs['nrows'] * self.specs['cellsize'],
            self.specs['cellsize']
        )[:self.specs['nrows']]

        # Reverse latitude to have it in descending order (north to south)
        self.lat = self.lat[::-1]

    def read_bil_file(self, file_path: Path) -> Tuple[np.ndarray, Dict]:
        """
        Read a PRISM BIL file and return data with metadata

        Parameters:
        -----------
        file_path : Path
            Path to BIL zip or bil file

        Returns:
        --------
        Tuple[np.ndarray, Dict]: Data array and metadata
        """
        dataset = self.processor.read_prism_dataset(file_path)
        data = dataset['data']
        metadata = dataset['metadata']

        # Don't flip data - keep original orientation
        # The latitude array is already reversed to be north-to-south

        return data, metadata

    def create_xarray_dataset(self, data: np.ndarray, metadata: Dict,
                             variable_name: str, time_stamp: datetime) -> xr.Dataset:
        """
        Create xarray Dataset from PRISM data

        Parameters:
        -----------
        data : np.ndarray
            2D data array
        metadata : Dict
            PRISM metadata
        variable_name : str
            Variable name (e.g., 'tmin', 'ppt')
        time_stamp : datetime
            Time stamp for the data

        Returns:
        --------
        xr.Dataset: Dataset with proper coordinates and attributes
        """
        # Expand data to 3D with time dimension
        data_3d = np.expand_dims(data, axis=0)

        # Create data array
        da = xr.DataArray(
            data_3d,
            coords={
                'time': [time_stamp],
                'lat': self.lat,
                'lon': self.lon
            },
            dims=['time', 'lat', 'lon'],
            name=variable_name,
            attrs=self.VARIABLE_INFO.get(variable_name, {})
        )

        # Convert to dataset
        ds = da.to_dataset()

        # Add global attributes
        ds.attrs = {
            'source': 'PRISM Climate Group, Oregon State University',
            'resolution': self.resolution,
            'creation_date': datetime.now().isoformat(),
            'original_format': 'BIL',
            'projection': 'NAD83',
            'nodata_value': self.specs['nodata_value']
        }

        # Add coordinate attributes
        ds['lat'].attrs = {
            'units': 'degrees_north',
            'long_name': 'Latitude',
            'standard_name': 'latitude'
        }

        ds['lon'].attrs = {
            'units': 'degrees_east',
            'long_name': 'Longitude',
            'standard_name': 'longitude'
        }

        ds['time'].attrs = {
            'long_name': 'Time',
            'standard_name': 'time'
        }

        return ds

    def process_time_series(self, input_dir: Path, variable: str,
                          start_date: datetime, end_date: datetime,
                          output_zarr: Path, batch_days: int = 365) -> None:
        """
        Process a time series of PRISM files and write to Zarr

        Parameters:
        -----------
        input_dir : Path
            Directory containing PRISM files
        variable : str
            Variable name to process
        start_date : datetime
            Start date
        end_date : datetime
            End date
        output_zarr : Path
            Output Zarr store path
        batch_days : int
            Number of days to process at once
        """
        logger.info(f"Processing {variable} from {start_date.date()} to {end_date.date()}")

        # Create output directory
        output_zarr.parent.mkdir(parents=True, exist_ok=True)

        # Check if zarr store exists for appending
        append_mode = output_zarr.exists()

        # Get list of files to process
        pattern = f"PRISM_{variable}_stable_4km*_bil.zip"
        all_files = sorted(input_dir.glob(pattern))

        if not all_files:
            logger.warning(f"No files found matching pattern: {pattern}")
            return

        # Filter files by date range
        files_to_process = []
        for file_path in all_files:
            try:
                # Parse date from filename
                metadata = self.processor.parse_filename(file_path.name)
                if 'year' in metadata and 'month' in metadata and 'day' in metadata:
                    file_date = datetime(metadata['year'], metadata['month'], metadata['day'])
                    if start_date <= file_date <= end_date:
                        files_to_process.append((file_path, file_date))
            except Exception as e:
                logger.warning(f"Could not parse date from {file_path.name}: {e}")

        if not files_to_process:
            logger.warning(f"No files found in date range {start_date.date()} to {end_date.date()}")
            return

        logger.info(f"Found {len(files_to_process)} files to process")

        # Track if any data was written
        data_written = False

        # Process in batches
        for i in range(0, len(files_to_process), batch_days):
            batch = files_to_process[i:i + batch_days]
            logger.info(f"Processing batch {i//batch_days + 1}/{(len(files_to_process) + batch_days - 1)//batch_days}")

            # Collect datasets for this batch
            datasets = []
            for file_path, file_date in tqdm(batch, desc=f"Reading {variable} files"):
                try:
                    data, metadata = self.read_bil_file(file_path)
                    ds = self.create_xarray_dataset(data, metadata, variable, file_date)
                    datasets.append(ds)
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    continue

            if not datasets:
                logger.warning("No valid datasets in this batch")
                continue

            # Concatenate datasets along time dimension
            batch_ds = xr.concat(datasets, dim='time')

            # Sort by time
            batch_ds = batch_ds.sortby('time')

            # Set encoding for chunking (let xarray handle compression by default)
            encoding = {
                variable: {
                    'chunks': (min(self.chunk_config['time'], len(batch_ds.time)),
                              self.chunk_config['lat'],
                              self.chunk_config['lon'])
                }
            }

            # Write to zarr
            if i == 0 and not append_mode:
                # First batch - create new store
                logger.info(f"Creating new Zarr store at {output_zarr}")
                batch_ds.to_zarr(output_zarr, mode='w', encoding=encoding)
                data_written = True
            else:
                # Append to existing store
                logger.info(f"Appending to Zarr store at {output_zarr}")
                batch_ds.to_zarr(output_zarr, mode='a', append_dim='time', encoding=encoding)
                data_written = True

            # Clean up memory
            del datasets, batch_ds

        # Only consolidate metadata if data was written
        if data_written:
            # Consolidate metadata for faster reads
            logger.info("Consolidating Zarr metadata")
            zarr.consolidate_metadata(str(output_zarr))
            logger.info(f"Successfully created Zarr store at {output_zarr}")
        else:
            logger.warning("No data was written to Zarr store")

    def convert_directory(self, input_dir: Path, output_base: Path,
                         start_date: Optional[datetime] = None,
                         end_date: Optional[datetime] = None) -> None:
        """
        Convert all variables in a directory to Zarr stores

        Parameters:
        -----------
        input_dir : Path
            Input directory containing PRISM data subdirectories
        output_base : Path
            Base output directory for Zarr stores
        start_date : Optional[datetime]
            Start date (uses all data if None)
        end_date : Optional[datetime]
            End date (uses all data if None)
        """
        # Get list of variable directories
        var_dirs = [d for d in input_dir.iterdir() if d.is_dir()]

        for var_dir in var_dirs:
            variable = var_dir.name
            if variable not in self.VARIABLE_INFO:
                logger.warning(f"Unknown variable: {variable}, skipping")
                continue

            # Determine date range if not specified
            if start_date is None or end_date is None:
                files = list(var_dir.glob("*.zip"))
                if not files:
                    logger.warning(f"No files found for {variable}")
                    continue

                # Parse dates from filenames to find range
                dates = []
                for f in files[:100]:  # Sample first 100 files
                    try:
                        metadata = self.processor.parse_filename(f.name)
                        if 'year' in metadata and 'month' in metadata and 'day' in metadata:
                            dates.append(datetime(metadata['year'], metadata['month'], metadata['day']))
                    except:
                        continue

                if dates:
                    start_date = start_date or min(dates)
                    end_date = end_date or max(dates)

            # Create output path
            output_zarr = output_base / f"{variable}_{start_date.year}_{end_date.year}.zarr"

            # Process this variable
            self.process_time_series(var_dir, variable, start_date, end_date, output_zarr)

    def validate_zarr(self, zarr_path: Path, original_file: Path,
                     variable: str, tolerance: float = 1e-5) -> bool:
        """
        Validate Zarr data against original BIL file

        Parameters:
        -----------
        zarr_path : Path
            Path to Zarr store
        original_file : Path
            Path to original BIL file for comparison
        variable : str
            Variable name
        tolerance : float
            Numerical tolerance for comparison

        Returns:
        --------
        bool: True if validation passes
        """
        try:
            # Read original data
            original_data, metadata = self.read_bil_file(original_file)

            # Parse date from filename
            metadata = self.processor.parse_filename(original_file.name)
            file_date = datetime(metadata['year'], metadata['month'], metadata['day'])

            # Read Zarr data for this date
            ds = xr.open_zarr(zarr_path)
            zarr_data = ds[variable].sel(time=file_date, method='nearest').values

            # Compare shapes
            if original_data.shape != zarr_data.shape:
                logger.error(f"Shape mismatch: {original_data.shape} vs {zarr_data.shape}")
                return False

            # Compare values (accounting for nodata)
            mask = original_data != self.specs['nodata_value']
            diff = np.abs(original_data[mask] - zarr_data[mask])
            max_diff = np.max(diff)

            if max_diff > tolerance:
                logger.error(f"Value mismatch: max difference = {max_diff}")
                return False

            logger.info(f"Validation passed for {original_file.name}")
            return True

        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    def get_zarr_info(self, zarr_path: Path) -> Dict:
        """
        Get information about a Zarr store

        Parameters:
        -----------
        zarr_path : Path
            Path to Zarr store

        Returns:
        --------
        Dict: Information about the Zarr store
        """
        ds = xr.open_zarr(zarr_path)

        info = {
            'variables': list(ds.data_vars),
            'dimensions': dict(ds.sizes),
            'time_range': [str(ds.time.values[0]), str(ds.time.values[-1])],
            'chunks': {},
            'size_mb': 0
        }

        # Get chunk info
        for var in ds.data_vars:
            if hasattr(ds[var], 'chunks'):
                info['chunks'][var] = ds[var].chunks

            # Get actual zarr array info
            z = zarr.open(str(zarr_path), mode='r')
            if var in z:
                arr = z[var]
                info['size_mb'] += arr.nbytes_stored / (1024 * 1024)

        ds.close()
        return info


def main():
    """
    Main function to demonstrate usage
    """
    print("="*60)
    print("PRISM to Zarr Converter")
    print("="*60)

    # Initialize converter
    converter = PRISMToZarrConverter(resolution='4km', chunk_strategy='time_optimized')

    # Example: Convert temperature data for 1981
    input_base = Path("./prism_daily_temp_1981_2000")
    output_base = Path("./zarr_stores")

    # Process just January 1981 as a test
    start_date = datetime(1981, 1, 1)
    end_date = datetime(1981, 1, 31)

    # Process tmin as an example
    if (input_base / "tmin").exists():
        output_zarr = output_base / "tmin_1981_01.zarr"
        converter.process_time_series(
            input_base / "tmin",
            "tmin",
            start_date,
            end_date,
            output_zarr,
            batch_days=31
        )

        # Get info about created store
        if output_zarr.exists():
            info = converter.get_zarr_info(output_zarr)
            print("\nZarr Store Information:")
            print(f"  Variables: {info['variables']}")
            print(f"  Dimensions: {info['dimensions']}")
            print(f"  Time range: {info['time_range']}")
            print(f"  Size: {info['size_mb']:.2f} MB")

            # Validate a sample file
            sample_files = list((input_base / "tmin").glob("*19810101*.zip"))
            if sample_files:
                print("\nValidating against original data...")
                valid = converter.validate_zarr(output_zarr, sample_files[0], "tmin")
                print(f"  Validation: {'PASSED' if valid else 'FAILED'}")
    else:
        print("No tmin data found. Please run download scripts first.")

    print("\n" + "="*60)
    print("Conversion complete!")
    print("="*60)


if __name__ == "__main__":
    main()