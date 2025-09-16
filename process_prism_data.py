#!/usr/bin/env python3
"""
PRISM Data Processing Examples
Demonstrates how to read and process PRISM BIL format data
"""

import os
import sys
import zipfile
import numpy as np
from pathlib import Path
from datetime import datetime
import struct

class PRISMProcessor:
    """
    Class to process PRISM climate data files
    """

    # PRISM grid specifications
    GRID_SPECS = {
        '4km': {
            'ncols': 1405,
            'nrows': 621,
            'xllcorner': -125.0208333,
            'yllcorner': 24.0625,
            'cellsize': 0.04166666666667,
            'nodata_value': -9999.0
        },
        '800m': {
            'ncols': 7025,
            'nrows': 3105,
            'xllcorner': -125.0208333,
            'yllcorner': 24.0625,
            'cellsize': 0.00833333333333,
            'nodata_value': -9999.0
        }
    }

    def __init__(self, resolution='4km'):
        """
        Initialize processor with grid specifications

        Parameters:
        -----------
        resolution : str
            Spatial resolution ('4km' or '800m')
        """
        self.resolution = resolution
        self.specs = self.GRID_SPECS[resolution]

    def extract_zip(self, zip_path, extract_to=None):
        """
        Extract a PRISM zip file

        Parameters:
        -----------
        zip_path : Path or str
            Path to the zip file
        extract_to : Path or str
            Directory to extract to

        Returns:
        --------
        list : List of extracted file paths
        """
        zip_path = Path(zip_path)

        if extract_to is None:
            extract_to = zip_path.parent / zip_path.stem

        extract_to = Path(extract_to)
        extract_to.mkdir(exist_ok=True)

        extracted_files = []

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                zip_ref.extract(member, extract_to)
                extracted_files.append(extract_to / member)

        print(f"Extracted {len(extracted_files)} files from {zip_path.name}")
        return extracted_files

    def read_bil_header(self, hdr_path):
        """
        Read PRISM BIL header file (.hdr)

        Parameters:
        -----------
        hdr_path : Path or str
            Path to the .hdr file

        Returns:
        --------
        dict : Header parameters
        """
        header = {}

        with open(hdr_path, 'r') as f:
            for line in f:
                if line.strip():
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        key = parts[0].lower()
                        value = ' '.join(parts[1:])
                        try:
                            # Try to convert to number
                            if '.' in value:
                                header[key] = float(value)
                            else:
                                header[key] = int(value)
                        except ValueError:
                            header[key] = value

        return header

    def read_bil_data(self, bil_path):
        """
        Read PRISM BIL binary data file

        Parameters:
        -----------
        bil_path : Path or str
            Path to the .bil file

        Returns:
        --------
        numpy.ndarray : 2D array of climate data
        """
        bil_path = Path(bil_path)

        # Check for header file
        hdr_path = bil_path.with_suffix('.hdr')
        if hdr_path.exists():
            header = self.read_bil_header(hdr_path)
            ncols = header.get('ncols', self.specs['ncols'])
            nrows = header.get('nrows', self.specs['nrows'])
            nodata = header.get('nodata_value', self.specs['nodata_value'])
        else:
            ncols = self.specs['ncols']
            nrows = self.specs['nrows']
            nodata = self.specs['nodata_value']

        # Read binary data
        with open(bil_path, 'rb') as f:
            # PRISM uses 32-bit floating point, little-endian
            data = np.fromfile(f, dtype='<f4')

        # Reshape to 2D array
        data = data.reshape((nrows, ncols))

        # Mask nodata values
        data = np.ma.masked_where(data == nodata, data)

        return data

    def read_prism_dataset(self, file_path):
        """
        Read a PRISM dataset (handles both .zip and .bil files)

        Parameters:
        -----------
        file_path : Path or str
            Path to the PRISM file

        Returns:
        --------
        dict : Dictionary containing data array and metadata
        """
        file_path = Path(file_path)

        if file_path.suffix == '.zip':
            # Extract and find .bil file
            extracted = self.extract_zip(file_path)
            bil_files = [f for f in extracted if f.suffix == '.bil']

            if not bil_files:
                raise ValueError(f"No .bil file found in {file_path}")

            bil_path = bil_files[0]
        elif file_path.suffix == '.bil':
            bil_path = file_path
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

        # Read data
        data = self.read_bil_data(bil_path)

        # Parse metadata from filename
        metadata = self.parse_filename(bil_path.name)

        # Add grid specifications
        metadata.update({
            'ncols': self.specs['ncols'],
            'nrows': self.specs['nrows'],
            'xllcorner': self.specs['xllcorner'],
            'yllcorner': self.specs['yllcorner'],
            'cellsize': self.specs['cellsize'],
            'nodata_value': self.specs['nodata_value']
        })

        return {
            'data': data,
            'metadata': metadata,
            'file_path': str(bil_path)
        }

    def parse_filename(self, filename):
        """
        Parse PRISM filename to extract metadata

        Parameters:
        -----------
        filename : str
            PRISM filename

        Returns:
        --------
        dict : Metadata extracted from filename
        """
        # Example: PRISM_ppt_stable_4kmM3_197101_bil.bil
        parts = filename.replace('.bil', '').split('_')

        metadata = {}

        if len(parts) >= 5:
            metadata['source'] = parts[0]  # PRISM
            metadata['variable'] = parts[1]  # ppt, tmin, tmax, etc.
            metadata['stability'] = parts[2]  # stable, provisional, etc.
            metadata['resolution'] = parts[3]  # 4kmM3, etc.
            metadata['date_str'] = parts[4]  # YYYYMM or YYYYMMDD

            # Parse date
            date_str = parts[4]
            if len(date_str) == 6:  # Monthly: YYYYMM
                metadata['temporal'] = 'monthly'
                metadata['year'] = int(date_str[:4])
                metadata['month'] = int(date_str[4:6])
            elif len(date_str) == 8:  # Daily: YYYYMMDD
                metadata['temporal'] = 'daily'
                metadata['year'] = int(date_str[:4])
                metadata['month'] = int(date_str[4:6])
                metadata['day'] = int(date_str[6:8])
            elif len(date_str) == 4:  # Annual: YYYY
                metadata['temporal'] = 'annual'
                metadata['year'] = int(date_str)

        return metadata

    def calculate_statistics(self, data):
        """
        Calculate basic statistics for PRISM data

        Parameters:
        -----------
        data : numpy.ndarray
            2D array of climate data

        Returns:
        --------
        dict : Statistics
        """
        valid_data = data[~data.mask] if hasattr(data, 'mask') else data

        stats = {
            'min': float(np.min(valid_data)),
            'max': float(np.max(valid_data)),
            'mean': float(np.mean(valid_data)),
            'std': float(np.std(valid_data)),
            'median': float(np.median(valid_data)),
            'count_valid': len(valid_data),
            'count_total': data.size
        }

        return stats

    def data_to_geotiff(self, data, output_path, metadata):
        """
        Convert PRISM data to GeoTIFF format (requires GDAL)

        Parameters:
        -----------
        data : numpy.ndarray
            2D array of climate data
        output_path : Path or str
            Output GeoTIFF file path
        metadata : dict
            Metadata including grid specifications
        """
        try:
            from osgeo import gdal, osr
        except ImportError:
            print("GDAL is required for GeoTIFF export. Install with: pip install gdal")
            return False

        # Create GeoTIFF
        driver = gdal.GetDriverByName('GTiff')
        dataset = driver.Create(
            str(output_path),
            metadata['ncols'],
            metadata['nrows'],
            1,
            gdal.GDT_Float32
        )

        # Set geotransform
        geotransform = (
            metadata['xllcorner'],  # top left x
            metadata['cellsize'],   # pixel width
            0,                      # rotation
            metadata['yllcorner'] + (metadata['nrows'] * metadata['cellsize']),  # top left y
            0,                      # rotation
            -metadata['cellsize']   # pixel height (negative)
        )
        dataset.SetGeoTransform(geotransform)

        # Set projection (NAD83 geographic)
        srs = osr.SpatialReference()
        srs.SetWellKnownGeogCS("NAD83")
        dataset.SetProjection(srs.ExportToWkt())

        # Write data
        band = dataset.GetRasterBand(1)
        band.WriteArray(data)
        band.SetNoDataValue(metadata['nodata_value'])

        # Close dataset
        dataset = None

        print(f"Saved GeoTIFF to {output_path}")
        return True

    def extract_point_value(self, data, lat, lon, metadata):
        """
        Extract value at a specific latitude/longitude

        Parameters:
        -----------
        data : numpy.ndarray
            2D array of climate data
        lat : float
            Latitude
        lon : float
            Longitude
        metadata : dict
            Metadata including grid specifications

        Returns:
        --------
        float : Value at the specified location
        """
        # Convert lat/lon to grid indices
        col = int((lon - metadata['xllcorner']) / metadata['cellsize'])
        row = int((metadata['yllcorner'] + (metadata['nrows'] * metadata['cellsize']) - lat) / metadata['cellsize'])

        # Check bounds
        if 0 <= row < metadata['nrows'] and 0 <= col < metadata['ncols']:
            return float(data[row, col])
        else:
            return None


def main():
    """
    Example usage of PRISM data processor
    """
    print("="*60)
    print("PRISM Data Processing Examples")
    print("="*60)

    # Initialize processor
    processor = PRISMProcessor(resolution='4km')

    # Example 1: Read and analyze a single file
    print("\nExample 1: Reading PRISM Data")
    print("-"*40)

    # Create sample data for demonstration
    sample_file = Path("./prism_1971_data/ppt/PRISM_ppt_4km_monthly_197101.zip")

    if sample_file.exists():
        # Read dataset
        dataset = processor.read_prism_dataset(sample_file)
        data = dataset['data']
        metadata = dataset['metadata']

        print(f"Variable: {metadata['variable']}")
        print(f"Date: {metadata['year']}-{metadata['month']:02d}")
        print(f"Shape: {data.shape}")

        # Calculate statistics
        stats = processor.calculate_statistics(data)
        print(f"\nStatistics:")
        print(f"  Min: {stats['min']:.2f}")
        print(f"  Max: {stats['max']:.2f}")
        print(f"  Mean: {stats['mean']:.2f}")
        print(f"  Std Dev: {stats['std']:.2f}")
        print(f"  Valid cells: {stats['count_valid']:,} / {stats['count_total']:,}")

        # Example 2: Extract point value
        print("\nExample 2: Extract Value at Location")
        print("-"*40)

        # Portland, OR coordinates
        lat, lon = 45.5152, -122.6784
        value = processor.extract_point_value(data, lat, lon, metadata)

        if value is not None:
            print(f"Location: Portland, OR ({lat:.4f}, {lon:.4f})")
            print(f"Value: {value:.2f} mm (precipitation)")
        else:
            print("Location outside data bounds")

        # Example 3: Export to GeoTIFF
        print("\nExample 3: Export to GeoTIFF")
        print("-"*40)

        output_tiff = Path("./prism_1971_01_ppt.tif")
        success = processor.data_to_geotiff(data, output_tiff, metadata)

        if success:
            print(f"Successfully exported to {output_tiff}")
    else:
        print(f"Sample file not found: {sample_file}")
        print("Please run download_1971_example.py first to download sample data")

    print("\n" + "="*60)
    print("Processing complete!")
    print("="*60)


if __name__ == "__main__":
    main()