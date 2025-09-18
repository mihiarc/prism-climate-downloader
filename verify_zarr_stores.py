#!/usr/bin/env python3
"""
Verify the created Zarr stores for PRISM temperature data
"""

import xarray as xr
from pathlib import Path
import zarr

def verify_zarr_stores():
    """
    Verify the created Zarr stores
    """
    zarr_dir = Path("/home/mihiarc/prism/zarr_stores")

    print("="*70)
    print("ZARR STORE VERIFICATION")
    print("="*70)

    # List of stores to check
    stores = [
        "tmin_1981_2000.zarr",
        "tmax_1981_2000.zarr",
        "tmean_1981_2000.zarr"
    ]

    for store_name in stores:
        store_path = zarr_dir / store_name
        if store_path.exists():
            print(f"\n{store_name}:")
            print("-" * 40)

            try:
                # Open the zarr store
                ds = xr.open_zarr(store_path)

                # Get basic info
                print(f"Variables: {list(ds.data_vars)}")
                print(f"Dimensions: {dict(ds.dims)}")

                # Time range
                time_values = ds.time.values
                print(f"Time range: {time_values[0]} to {time_values[-1]}")
                print(f"Number of time steps: {len(time_values)}")

                # Check for each variable
                for var in ds.data_vars:
                    da = ds[var]
                    print(f"\n  {var}:")
                    print(f"    Shape: {da.shape}")
                    print(f"    Chunks: {da.chunks if hasattr(da, 'chunks') else 'N/A'}")

                    # Get non-NaN stats
                    data_sample = da.isel(time=0).values
                    valid_data = data_sample[data_sample != -9999]  # PRISM nodata value
                    if len(valid_data) > 0:
                        print(f"    Min value: {valid_data.min():.2f}")
                        print(f"    Max value: {valid_data.max():.2f}")
                        print(f"    Mean value: {valid_data.mean():.2f}")

                # Get store size
                z = zarr.open(str(store_path), mode='r')
                total_size = 0
                for array_name in z.array_keys():
                    arr = z[array_name]
                    total_size += arr.nbytes_stored
                print(f"\nStore size: {total_size / (1024**2):.2f} MB")

                ds.close()

            except Exception as e:
                print(f"Error reading store: {e}")
        else:
            print(f"\n{store_name}: NOT FOUND")

    print("\n" + "="*70)
    print("Verification complete!")
    print("="*70)

if __name__ == "__main__":
    verify_zarr_stores()