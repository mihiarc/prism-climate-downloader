#!/usr/bin/env python3
"""
Integration test for PRISM Zarr store visualization
Tests data loading and creates map visualizations for inspection
"""

import os
import sys
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


class ZarrIntegrationTest:
    """Integration test for PRISM Zarr stores with visualization"""

    def __init__(self, zarr_path: str):
        """Initialize test with zarr store path"""
        self.zarr_path = Path(zarr_path)
        if not self.zarr_path.exists():
            raise FileNotFoundError(f"Zarr store not found: {zarr_path}")

        print(f"Loading Zarr store: {self.zarr_path}")
        self.ds = xr.open_zarr(self.zarr_path)
        self.variable = list(self.ds.data_vars)[0]  # Get first variable

    def test_data_integrity(self):
        """Test basic data integrity"""
        print("\n" + "="*60)
        print("DATA INTEGRITY TESTS")
        print("="*60)

        # Test 1: Check dimensions
        print("\n1. Checking dimensions...")
        assert 'time' in self.ds.dims, "Missing time dimension"
        assert 'lat' in self.ds.dims, "Missing lat dimension"
        assert 'lon' in self.ds.dims, "Missing lon dimension"
        print(f"   ✓ Dimensions: {dict(self.ds.dims)}")

        # Test 2: Check coordinate ranges
        print("\n2. Checking coordinate ranges...")
        lat_range = (self.ds.lat.min().values, self.ds.lat.max().values)
        lon_range = (self.ds.lon.min().values, self.ds.lon.max().values)
        print(f"   ✓ Latitude range: {lat_range[0]:.2f} to {lat_range[1]:.2f}")
        print(f"   ✓ Longitude range: {lon_range[0]:.2f} to {lon_range[1]:.2f}")
        assert 20 < lat_range[0] < 50, "Latitude out of CONUS range"
        assert -130 < lon_range[0] < -60, "Longitude out of CONUS range"

        # Test 3: Check data variable
        print(f"\n3. Checking variable '{self.variable}'...")
        data = self.ds[self.variable]
        print(f"   ✓ Shape: {data.shape}")
        print(f"   ✓ Data type: {data.dtype}")

        # Test 4: Check for valid data
        print("\n4. Checking data validity...")
        sample = data.isel(time=0).values
        # Check for both nodata and NaN values
        valid_mask = (sample != -9999.0) & np.isfinite(sample)
        valid_pct = (valid_mask.sum() / valid_mask.size) * 100
        print(f"   ✓ Valid data: {valid_pct:.1f}%")
        assert valid_pct > 50, f"Too much missing data: {valid_pct:.1f}%"

        # Test 5: Check value ranges
        print("\n5. Checking value ranges...")
        valid_data = sample[valid_mask]
        if len(valid_data) > 0:
            stats = {
                'min': np.nanmin(valid_data),
                'max': np.nanmax(valid_data),
                'mean': np.nanmean(valid_data),
                'std': np.nanstd(valid_data)
            }
        else:
            stats = {'min': 0, 'max': 0, 'mean': 0, 'std': 0}
        print(f"   ✓ Min: {stats['min']:.2f}°C")
        print(f"   ✓ Max: {stats['max']:.2f}°C")
        print(f"   ✓ Mean: {stats['mean']:.2f}°C")
        print(f"   ✓ Std: {stats['std']:.2f}°C")

        # Reasonable temperature bounds for CONUS
        assert -50 < stats['min'] < 50, f"Unrealistic minimum temperature: {stats['min']}"
        assert -50 < stats['max'] < 50, f"Unrealistic maximum temperature: {stats['max']}"

        print("\n✅ All integrity tests passed!")
        return True

    def create_overview_maps(self, output_dir: str = "./test_outputs"):
        """Create overview maps for visual inspection"""
        print("\n" + "="*60)
        print("CREATING VISUALIZATION MAPS")
        print("="*60)

        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        # Select dates for visualization
        dates_to_plot = [
            self.ds.time[0],   # First day
            self.ds.time[len(self.ds.time)//2],  # Middle day
            self.ds.time[-1]   # Last day
        ]

        # Create figure with multiple subplots
        fig = plt.figure(figsize=(20, 15))

        for idx, date in enumerate(dates_to_plot, 1):
            print(f"\nProcessing {date.values}...")

            # Get data for this date
            data = self.ds[self.variable].sel(time=date)
            data_values = data.values

            # Mask nodata
            data_masked = np.ma.masked_equal(data_values, -9999.0)

            # Create subplot with map projection
            ax = fig.add_subplot(2, 3, idx, projection=ccrs.PlateCarree())

            # Add map features
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax.add_feature(cfeature.BORDERS, linewidth=0.5)
            ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor='gray')
            ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.3)
            ax.add_feature(cfeature.LAKES, color='lightblue', alpha=0.3)

            # Plot data
            im = ax.pcolormesh(
                self.ds.lon, self.ds.lat, data_masked,
                transform=ccrs.PlateCarree(),
                cmap='RdBu_r',
                vmin=np.percentile(data_masked.compressed(), 5),
                vmax=np.percentile(data_masked.compressed(), 95)
            )

            # Set extent to CONUS
            ax.set_extent([-125, -66.5, 24, 50], ccrs.PlateCarree())

            # Add gridlines
            gl = ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
            gl.top_labels = False
            gl.right_labels = False

            # Title
            date_str = pd.Timestamp(date.values).strftime('%Y-%m-%d')
            ax.set_title(f'{self.variable.upper()} - {date_str}', fontsize=12, fontweight='bold')

            # Colorbar
            cbar = plt.colorbar(im, ax=ax, orientation='horizontal', pad=0.05, shrink=0.8)
            cbar.set_label('Temperature (°C)', fontsize=10)

        # Add statistics subplot
        ax_stats = fig.add_subplot(2, 3, 4)

        # Calculate time series statistics
        print("\nCalculating time series statistics...")
        mean_ts = []
        for t in self.ds.time:
            data_t = self.ds[self.variable].sel(time=t).values
            valid_data = data_t[data_t != -9999.0]
            if len(valid_data) > 0:
                mean_ts.append(np.mean(valid_data))
            else:
                mean_ts.append(np.nan)

        # Plot time series
        time_vals = pd.to_datetime(self.ds.time.values)
        ax_stats.plot(time_vals, mean_ts, 'b-', linewidth=2)
        ax_stats.set_xlabel('Date', fontsize=10)
        ax_stats.set_ylabel(f'Mean {self.variable.upper()} (°C)', fontsize=10)
        ax_stats.set_title('Spatial Mean Time Series', fontsize=12, fontweight='bold')
        ax_stats.grid(True, alpha=0.3)
        ax_stats.tick_params(axis='x', rotation=45)

        # Add histogram subplot
        ax_hist = fig.add_subplot(2, 3, 5)

        # Get all valid data for histogram
        all_data = self.ds[self.variable].values.flatten()
        valid_data = all_data[all_data != -9999.0]

        # Create histogram
        ax_hist.hist(valid_data, bins=50, color='steelblue', edgecolor='black', alpha=0.7)
        ax_hist.set_xlabel(f'{self.variable.upper()} (°C)', fontsize=10)
        ax_hist.set_ylabel('Frequency', fontsize=10)
        ax_hist.set_title('Value Distribution (All Times)', fontsize=12, fontweight='bold')
        ax_hist.grid(True, alpha=0.3)

        # Add text statistics
        ax_text = fig.add_subplot(2, 3, 6)
        ax_text.axis('off')

        stats_text = f"""
        DATASET SUMMARY
        {'='*30}

        Variable: {self.variable.upper()}
        Time Range: {str(self.ds.time[0].values)[:10]} to {str(self.ds.time[-1].values)[:10]}
        Number of Time Steps: {len(self.ds.time)}

        Spatial Coverage:
        • Latitude: {self.ds.lat.min().values:.2f}° to {self.ds.lat.max().values:.2f}°
        • Longitude: {self.ds.lon.min().values:.2f}° to {self.ds.lon.max().values:.2f}°
        • Grid Size: {len(self.ds.lat)} × {len(self.ds.lon)}

        Data Statistics:
        • Min: {np.min(valid_data):.2f}°C
        • Max: {np.max(valid_data):.2f}°C
        • Mean: {np.mean(valid_data):.2f}°C
        • Median: {np.median(valid_data):.2f}°C
        • Std Dev: {np.std(valid_data):.2f}°C

        Data Quality:
        • Valid Points: {len(valid_data):,} ({(len(valid_data)/len(all_data)*100):.1f}%)
        • Missing Points: {len(all_data)-len(valid_data):,} ({((len(all_data)-len(valid_data))/len(all_data)*100):.1f}%)
        """

        ax_text.text(0.1, 0.9, stats_text, transform=ax_text.transAxes,
                    fontsize=10, verticalalignment='top', fontfamily='monospace')

        # Overall title
        fig.suptitle(f'PRISM Zarr Integration Test - {self.zarr_path.name}',
                    fontsize=16, fontweight='bold', y=0.98)

        # Save figure
        output_file = output_path / f"zarr_integration_test_{self.variable}.png"
        plt.tight_layout()
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"\n✅ Map saved to: {output_file}")

        # Also create a single-day detailed map
        self.create_detailed_map(self.ds.time[15], output_path)

        plt.show()

    def create_detailed_map(self, date, output_dir):
        """Create a detailed map for a single date"""
        print(f"\nCreating detailed map for {date.values}...")

        fig = plt.figure(figsize=(14, 10))
        ax = plt.axes(projection=ccrs.PlateCarree())

        # Get data
        data = self.ds[self.variable].sel(time=date)
        data_masked = np.ma.masked_equal(data.values, -9999.0)

        # Add detailed features
        ax.add_feature(cfeature.LAND, color='lightgray', alpha=0.3)
        ax.add_feature(cfeature.OCEAN, color='azure', alpha=0.5)
        ax.add_feature(cfeature.COASTLINE, linewidth=1, edgecolor='black')
        ax.add_feature(cfeature.BORDERS, linewidth=1, edgecolor='black')
        ax.add_feature(cfeature.STATES, linewidth=0.5, edgecolor='gray')
        ax.add_feature(cfeature.LAKES, color='azure', alpha=0.5)
        ax.add_feature(cfeature.RIVERS, color='azure', alpha=0.5)

        # Plot with contours
        im = ax.pcolormesh(
            self.ds.lon, self.ds.lat, data_masked,
            transform=ccrs.PlateCarree(),
            cmap='RdBu_r',
            shading='auto'
        )

        # Add contours
        contours = ax.contour(
            self.ds.lon, self.ds.lat, data_masked,
            levels=10,
            colors='black',
            linewidths=0.5,
            alpha=0.3,
            transform=ccrs.PlateCarree()
        )

        # Set extent
        ax.set_extent([-125, -66.5, 24, 50], ccrs.PlateCarree())

        # Gridlines with labels
        gl = ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5, linestyle='--')
        gl.top_labels = False
        gl.right_labels = False

        # Title and labels
        date_str = pd.Timestamp(date.values).strftime('%B %d, %Y')
        plt.title(f'PRISM {self.variable.upper()} - {date_str}', fontsize=14, fontweight='bold', pad=20)

        # Colorbar
        cbar = plt.colorbar(im, ax=ax, orientation='vertical', pad=0.02, shrink=0.8)
        cbar.set_label('Temperature (°C)', fontsize=11)

        # Save
        output_file = output_dir / f"zarr_detailed_map_{self.variable}_{pd.Timestamp(date.values).strftime('%Y%m%d')}.png"
        plt.savefig(output_file, dpi=200, bbox_inches='tight')
        print(f"✅ Detailed map saved to: {output_file}")

    def run_all_tests(self):
        """Run all integration tests"""
        print("\n" + "="*60)
        print("PRISM ZARR INTEGRATION TEST SUITE")
        print("="*60)

        try:
            # Run data integrity tests
            self.test_data_integrity()

            # Create visualizations
            self.create_overview_maps()

            print("\n" + "="*60)
            print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
            print("="*60)
            return True

        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main test runner"""
    # Path to test zarr store
    zarr_path = "./zarr_stores/tmin_1981_01.zarr"

    # Check if zarr store exists
    if not Path(zarr_path).exists():
        print(f"❌ Zarr store not found at {zarr_path}")
        print("Please run prism_to_zarr.py first to create test data")
        sys.exit(1)

    # Run tests
    tester = ZarrIntegrationTest(zarr_path)
    success = tester.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()