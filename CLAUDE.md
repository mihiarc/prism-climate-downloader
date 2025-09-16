# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository contains Python tools for bulk downloading PRISM (Parameter-elevation Regressions on Independent Slopes Model) climate data from Oregon State University. PRISM provides gridded climate data for the continental United States at 4km and 800m resolutions.

## Key Commands

### Testing Downloads
```bash
# Test temperature download (3 days)
python3 test_daily_download.py

# Test non-temperature variables download (3 days)
python3 test_daily_other_download.py
```

### Production Downloads
```bash
# Download daily temperature data (1981-2000)
python3 download_daily_temp_1981_2000.py

# Download daily non-temperature data (1981-2000)
python3 download_daily_other_1981_2000.py

# General bulk download with custom parameters
python3 prism_bulk_download.py --temporal daily --start 2020-01-01 --end 2020-12-31 --variables ppt tmin tmax --output-dir ./prism_data
```

### Processing Data
```bash
# Process downloaded PRISM data
python3 process_prism_data.py
```

## Architecture

### Core Scripts

1. **prism_bulk_download.py**: Main flexible downloader with PRISMDownloader class
   - Supports all temporal resolutions (daily, monthly, annual)
   - Configurable date ranges and variables
   - Parallel downloads with retry logic

2. **download_daily_temp_1981_2000.py**: Specialized for temperature variables (tmin, tmax, tmean)
   - PRISMDailyDownloader class
   - Optimized for 1981-2000 daily temperature data
   - Automatic fallback from HTTP to FTP

3. **download_daily_other_1981_2000.py**: Specialized for non-temperature variables
   - PRISMDailyOtherDownloader class
   - Downloads ppt, tdmean, vpdmin, vpdmax
   - Interactive variable selection

4. **process_prism_data.py**: Data processing utilities
   - PRISMProcessor class
   - Handles BIL format extraction
   - Statistical calculations and point value extraction

### Download Strategy

- Primary: HTTPS via services.nacse.org/prism/data/public/4km
- Fallback: FTP via prism.oregonstate.edu/daily
- Concurrent downloads (default 4 workers)
- Automatic retry with exponential backoff
- Resume capability (skips existing files)

### Data Organization

Downloaded files are organized as:
```
output_dir/
├── variable_name/
│   ├── PRISM_variable_stable_4kmD2_YYYYMMDD_bil.zip
│   └── ...
└── download_log.txt
```

## PRISM Data Details

### Available Variables
- **Temperature**: tmin, tmax, tmean (°C)
- **Precipitation**: ppt (mm)
- **Humidity**: tdmean (dew point °C), vpdmin, vpdmax (vapor pressure deficit hPa)

### Data Availability
- **Daily**: 1981-present
- **Monthly**: 1895-present
- **Annual**: 1895-present

### File Formats
- BIL (Band Interleaved by Line) raster format
- Compressed as ZIP with metadata (.hdr, .prj, .xml)
- 4km resolution (~0.04167 degrees)