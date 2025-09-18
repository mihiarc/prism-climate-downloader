#!/usr/bin/env python3
"""
PRISM Zarr Analysis Utilities
Tools for efficient analysis of PRISM climate data stored in Zarr format
"""

import numpy as np
import xarray as xr
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Union
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PRISMZarrAnalyzer:
    """
    Analysis utilities for PRISM data in Zarr format
    """

    def __init__(self, zarr_path: Union[str, Path]):
        """
        Initialize analyzer with a Zarr store

        Parameters:
        -----------
        zarr_path : Union[str, Path]
            Path to the Zarr store
        """
        self.zarr_path = Path(zarr_path)
        if not self.zarr_path.exists():
            raise FileNotFoundError(f"Zarr store not found: {self.zarr_path}")

        # Open the dataset
        self.ds = xr.open_zarr(self.zarr_path)
        self.variables = list(self.ds.data_vars)
        logger.info(f"Loaded Zarr store with variables: {self.variables}")

    def __del__(self):
        """Clean up by closing the dataset"""
        if hasattr(self, 'ds'):
            self.ds.close()

    def extract_point_time_series(self, lat: float, lon: float,
                                 variable: Optional[str] = None,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Extract time series for a specific location

        Parameters:
        -----------
        lat : float
            Latitude
        lon : float
            Longitude
        variable : Optional[str]
            Variable name (uses first available if None)
        start_date : Optional[datetime]
            Start date for extraction
        end_date : Optional[datetime]
            End date for extraction

        Returns:
        --------
        pd.DataFrame: Time series data
        """
        if variable is None:
            variable = self.variables[0]
        elif variable not in self.variables:
            raise ValueError(f"Variable {variable} not found. Available: {self.variables}")

        # Select the nearest point
        point_data = self.ds[variable].sel(lat=lat, lon=lon, method='nearest')

        # Filter by date if specified
        if start_date or end_date:
            time_slice = slice(start_date, end_date)
            point_data = point_data.sel(time=time_slice)

        # Convert to DataFrame
        df = point_data.to_dataframe(name=variable)
        df = df.reset_index()

        # Add location info
        actual_lat = float(point_data.lat.values)
        actual_lon = float(point_data.lon.values)
        df['actual_lat'] = actual_lat
        df['actual_lon'] = actual_lon
        df['requested_lat'] = lat
        df['requested_lon'] = lon

        logger.info(f"Extracted {len(df)} time points for location ({actual_lat:.4f}, {actual_lon:.4f})")
        return df

    def extract_region_average(self, lat_bounds: Tuple[float, float],
                              lon_bounds: Tuple[float, float],
                              variable: Optional[str] = None,
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Extract spatial average for a region over time

        Parameters:
        -----------
        lat_bounds : Tuple[float, float]
            (min_lat, max_lat)
        lon_bounds : Tuple[float, float]
            (min_lon, max_lon)
        variable : Optional[str]
            Variable name
        start_date : Optional[datetime]
            Start date
        end_date : Optional[datetime]
            End date

        Returns:
        --------
        pd.DataFrame: Regional average time series
        """
        if variable is None:
            variable = self.variables[0]

        # Select region
        region_data = self.ds[variable].sel(
            lat=slice(lat_bounds[1], lat_bounds[0]),  # Note: lat is typically in descending order
            lon=slice(lon_bounds[0], lon_bounds[1])
        )

        # Filter by date
        if start_date or end_date:
            time_slice = slice(start_date, end_date)
            region_data = region_data.sel(time=time_slice)

        # Calculate mean over space
        region_mean = region_data.mean(dim=['lat', 'lon'])

        # Convert to DataFrame
        df = region_mean.to_dataframe(name=f"{variable}_mean")
        df = df.reset_index()

        # Add region info
        df['lat_min'] = lat_bounds[0]
        df['lat_max'] = lat_bounds[1]
        df['lon_min'] = lon_bounds[0]
        df['lon_max'] = lon_bounds[1]
        df['n_grid_cells'] = len(region_data.lat) * len(region_data.lon)

        logger.info(f"Calculated regional average for {len(df)} time points")
        return df

    def compute_climatology(self, variable: Optional[str] = None,
                          groupby: str = 'month') -> xr.Dataset:
        """
        Compute climatology (long-term averages)

        Parameters:
        -----------
        variable : Optional[str]
            Variable name
        groupby : str
            Grouping period ('month', 'dayofyear', 'season')

        Returns:
        --------
        xr.Dataset: Climatology dataset
        """
        if variable is None:
            variable = self.variables[0]

        data = self.ds[variable]

        if groupby == 'month':
            climatology = data.groupby('time.month').mean()
        elif groupby == 'dayofyear':
            climatology = data.groupby('time.dayofyear').mean()
        elif groupby == 'season':
            climatology = data.groupby('time.season').mean()
        else:
            raise ValueError(f"Invalid groupby: {groupby}. Use 'month', 'dayofyear', or 'season'")

        logger.info(f"Computed {groupby} climatology for {variable}")
        return climatology

    def compute_anomalies(self, variable: Optional[str] = None,
                         reference_period: Optional[Tuple[str, str]] = None) -> xr.DataArray:
        """
        Compute anomalies from climatology

        Parameters:
        -----------
        variable : Optional[str]
            Variable name
        reference_period : Optional[Tuple[str, str]]
            Reference period for climatology (start_date, end_date)

        Returns:
        --------
        xr.DataArray: Anomaly data
        """
        if variable is None:
            variable = self.variables[0]

        data = self.ds[variable]

        # Calculate reference climatology
        if reference_period:
            ref_data = data.sel(time=slice(reference_period[0], reference_period[1]))
            climatology = ref_data.groupby('time.month').mean()
        else:
            climatology = data.groupby('time.month').mean()

        # Calculate anomalies
        anomalies = data.groupby('time.month') - climatology

        logger.info(f"Computed anomalies for {variable}")
        return anomalies

    def export_to_netcdf(self, output_path: Union[str, Path],
                        variables: Optional[List[str]] = None,
                        lat_bounds: Optional[Tuple[float, float]] = None,
                        lon_bounds: Optional[Tuple[float, float]] = None,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None) -> None:
        """
        Export subset of data to NetCDF

        Parameters:
        -----------
        output_path : Union[str, Path]
            Output NetCDF file path
        variables : Optional[List[str]]
            Variables to export (all if None)
        lat_bounds : Optional[Tuple[float, float]]
            Latitude bounds
        lon_bounds : Optional[Tuple[float, float]]
            Longitude bounds
        start_date : Optional[datetime]
            Start date
        end_date : Optional[datetime]
            End date
        """
        # Select data subset
        subset = self.ds

        if variables:
            subset = subset[variables]

        if lat_bounds:
            subset = subset.sel(lat=slice(lat_bounds[1], lat_bounds[0]))

        if lon_bounds:
            subset = subset.sel(lon=slice(lon_bounds[0], lon_bounds[1]))

        if start_date or end_date:
            subset = subset.sel(time=slice(start_date, end_date))

        # Export to NetCDF
        subset.to_netcdf(output_path)
        logger.info(f"Exported data to {output_path}")

    def export_to_geotiff(self, output_dir: Union[str, Path],
                         variable: Optional[str] = None,
                         time_index: Optional[int] = None) -> None:
        """
        Export a single time slice to GeoTIFF

        Parameters:
        -----------
        output_dir : Union[str, Path]
            Output directory for GeoTIFF files
        variable : Optional[str]
            Variable to export
        time_index : Optional[int]
            Time index to export (default: 0)
        """
        try:
            import rioxarray
        except ImportError:
            logger.error("rioxarray is required for GeoTIFF export. Install with: uv pip install rioxarray")
            return

        if variable is None:
            variable = self.variables[0]

        if time_index is None:
            time_index = 0

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Select data
        data = self.ds[variable].isel(time=time_index)

        # Add CRS
        data = data.rio.write_crs("EPSG:4269")  # NAD83

        # Export
        time_str = pd.Timestamp(data.time.values).strftime('%Y%m%d')
        output_path = output_dir / f"{variable}_{time_str}.tif"
        data.rio.to_raster(output_path)

        logger.info(f"Exported GeoTIFF to {output_path}")

    def plot_time_series(self, lat: float, lon: float,
                        variable: Optional[str] = None,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        save_path: Optional[Union[str, Path]] = None) -> None:
        """
        Plot time series for a location

        Parameters:
        -----------
        lat : float
            Latitude
        lon : float
            Longitude
        variable : Optional[str]
            Variable to plot
        start_date : Optional[datetime]
            Start date
        end_date : Optional[datetime]
            End date
        save_path : Optional[Union[str, Path]]
            Path to save figure
        """
        df = self.extract_point_time_series(lat, lon, variable, start_date, end_date)

        if variable is None:
            variable = self.variables[0]

        # Create plot
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df['time'], df[variable], linewidth=1)
        ax.set_xlabel('Date')
        ax.set_ylabel(f'{variable}')
        ax.set_title(f'{variable} Time Series at ({lat:.2f}, {lon:.2f})')
        ax.grid(True, alpha=0.3)

        # Rotate x-axis labels
        plt.xticks(rotation=45)
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"Saved plot to {save_path}")
        else:
            plt.show()

    def get_statistics(self, variable: Optional[str] = None,
                       lat_bounds: Optional[Tuple[float, float]] = None,
                       lon_bounds: Optional[Tuple[float, float]] = None) -> Dict:
        """
        Get statistical summary of data

        Parameters:
        -----------
        variable : Optional[str]
            Variable name
        lat_bounds : Optional[Tuple[float, float]]
            Latitude bounds
        lon_bounds : Optional[Tuple[float, float]]
            Longitude bounds

        Returns:
        --------
        Dict: Statistical summary
        """
        if variable is None:
            variable = self.variables[0]

        data = self.ds[variable]

        # Select region if specified
        if lat_bounds:
            data = data.sel(lat=slice(lat_bounds[1], lat_bounds[0]))
        if lon_bounds:
            data = data.sel(lon=slice(lon_bounds[0], lon_bounds[1]))

        # Calculate statistics
        stats = {
            'variable': variable,
            'min': float(data.min().values),
            'max': float(data.max().values),
            'mean': float(data.mean().values),
            'std': float(data.std().values),
            'median': float(data.median().values),
            'shape': data.shape,
            'time_range': (str(data.time.values[0]), str(data.time.values[-1])),
            'lat_range': (float(data.lat.min()), float(data.lat.max())),
            'lon_range': (float(data.lon.min()), float(data.lon.max()))
        }

        return stats


def main():
    """
    Example usage of PRISM Zarr analysis utilities
    """
    print("="*60)
    print("PRISM Zarr Analysis Examples")
    print("="*60)

    # Check if Zarr store exists
    zarr_path = Path("./zarr_stores/tmin_1981_01.zarr")

    if not zarr_path.exists():
        print(f"Zarr store not found at {zarr_path}")
        print("Please run prism_to_zarr.py first to create the store")
        return

    # Initialize analyzer
    analyzer = PRISMZarrAnalyzer(zarr_path)

    # Example 1: Extract point time series
    print("\nExample 1: Point Time Series")
    print("-"*40)

    # Kansas City, MO
    lat, lon = 39.0997, -94.5786
    df = analyzer.extract_point_time_series(lat, lon)
    print(f"Location: Kansas City, MO")
    print(f"Time points: {len(df)}")
    print(f"Mean temperature: {df['tmin'].mean():.2f}°C")
    print(f"Min temperature: {df['tmin'].min():.2f}°C")
    print(f"Max temperature: {df['tmin'].max():.2f}°C")

    # Example 2: Regional average
    print("\nExample 2: Regional Average")
    print("-"*40)

    # Pacific Northwest region
    lat_bounds = (42.0, 49.0)
    lon_bounds = (-125.0, -116.0)
    regional_df = analyzer.extract_region_average(lat_bounds, lon_bounds)
    print(f"Region: Pacific Northwest")
    print(f"Mean temperature: {regional_df['tmin_mean'].mean():.2f}°C")

    # Example 3: Statistics
    print("\nExample 3: Data Statistics")
    print("-"*40)

    stats = analyzer.get_statistics()
    for key, value in stats.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")

    # Example 4: Export subset to NetCDF (optional - requires netCDF4)
    print("\nExample 4: Export to NetCDF")
    print("-"*40)

    try:
        output_nc = Path("./prism_subset.nc")
        analyzer.export_to_netcdf(
            output_nc,
            lat_bounds=(44.0, 46.0),
            lon_bounds=(-124.0, -121.0)
        )
        print(f"Exported subset to {output_nc}")
    except ValueError as e:
        print(f"NetCDF export skipped: {e}")
        print("To enable NetCDF export, install: uv pip install netCDF4")

    print("\n" + "="*60)
    print("Analysis complete!")
    print("="*60)


if __name__ == "__main__":
    main()