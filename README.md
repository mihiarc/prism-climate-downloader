# PRISM Climate Data Downloader

Python tools for bulk downloading PRISM (Parameter-elevation Regressions on Independent Slopes Model) climate data from Oregon State University.

## Overview

PRISM is a USDA official climatological dataset providing gridded climate data for the continental United States. This repository contains Python scripts to efficiently download PRISM data in bulk.

### Features

- 🚀 Parallel downloads with configurable workers
- 🔄 Automatic retry with exponential backoff
- 📊 Progress tracking and detailed logging
- 💾 Resume capability (skips existing files)
- 🗂️ Organized file structure by variable and time
- 🌡️ Support for all PRISM climate variables
- 📅 Historical data from 1895 (monthly) and 1981 (daily)

## Quick Start

### Prerequisites

```bash
# Optional but recommended for progress bars
pip install tqdm
```

### Download Daily Temperature Data (1981-2000)

```bash
python3 download_daily_temp_1981_2000.py
```

### Download Monthly Data (1971-Present)

```bash
python3 prism_bulk_download.py \
  --temporal monthly \
  --start 1971-01-01 \
  --end 2025-01-01 \
  --variables ppt tmin tmax tmean \
  --output-dir ./prism_data
```

## Available Scripts

### `prism_bulk_download.py`
Main script for bulk downloading any PRISM data with full customization options.

**Options:**
- `--temporal`: Data temporal resolution (`daily`, `monthly`, `annual`)
- `--start`: Start date (YYYY-MM-DD)
- `--end`: End date (YYYY-MM-DD)
- `--variables`: Climate variables to download (see list below)
- `--output-dir`: Output directory for downloaded files
- `--workers`: Number of parallel downloads (default: 4)

### `download_daily_temp_1981_2000.py`
Specialized script for downloading daily temperature data from 1981-2000.

### `process_prism_data.py`
Utilities for processing downloaded PRISM data:
- Extract ZIP files
- Read BIL format rasters
- Calculate statistics
- Extract point values

### `test_daily_download.py`
Test script that downloads 3 days of data to verify setup.

## Available Variables

### Temperature
- `tmin` - Minimum temperature (°C)
- `tmax` - Maximum temperature (°C)
- `tmean` - Mean temperature (°C)

### Precipitation
- `ppt` - Precipitation (mm)

### Humidity
- `tdmean` - Mean dew point temperature (°C)
- `vpdmin` - Minimum vapor pressure deficit (hPa)
- `vpdmax` - Maximum vapor pressure deficit (hPa)

## Data Availability

- **Monthly data**: 1895 to present
- **Daily data**: 1981 to present
- **Annual data**: 1895 to present
- **30-year normals**: 1991-2020 (current)

## Data Details

### Resolution
- Native: 800m
- Standard download: 4km
- Projection: Geographic (Lat/Long), NAD83

### File Format
- **BIL** (Band Interleaved by Line) - Primary format
- Compressed as ZIP files
- Includes metadata files (.hdr, .prj, .xml)

### File Naming Convention
```
PRISM_[variable]_[stability]_[resolution]_[date]_bil.zip
```
Example: `PRISM_ppt_stable_4kmM3_197101_bil.zip`

## Storage Requirements

- **Monthly data**: ~10-50 MB per variable per year (compressed)
- **Daily data**: ~2-5 GB per variable per year (compressed)
- **Full daily temperature 1981-2000**: ~40-45 GB

## Important Notes

1. **No authentication required** for FTP/HTTP access
2. **Be respectful** with parallel downloads (4-8 workers recommended)
3. **Data updates**:
   - Daily data: 1-day lag
   - Monthly data: Available after month completion
4. **Citations**: Please cite PRISM Climate Group when using this data

## Data Source

PRISM Climate Group
Oregon State University
https://prism.oregonstate.edu
prism-questions@nacse.org

## License

This software is provided as-is for downloading publicly available PRISM climate data. PRISM datasets are available without restriction on use or distribution. PRISM Climate Group requests appropriate citation.

## Citation

When using PRISM data, please cite:

PRISM Climate Group, Oregon State University, https://prism.oregonstate.edu, data created [date], accessed [date]