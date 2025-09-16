# PRISM Climate Data Bulk Download Guide

## Overview

PRISM (Parameter-elevation Regressions on Independent Slopes Model) provides high-quality gridded climate data for the continental United States. This guide covers setting up bulk downloads for PRISM data from 1971 to present.

## Data Availability

### Temporal Coverage

| Resolution | Start Date | End Date | Update Frequency |
|------------|------------|----------|------------------|
| **Monthly** | January 1895 | Present | Monthly |
| **Daily** | January 1, 1981 | Present | Daily |
| **Annual** | 1895 | Present | Annually |
| **30-year Normals** | 1991-2020 | - | Decadal |

### Climate Variables Available from 1971 (Monthly Data)

| Variable | Description | Units | Available Since |
|----------|-------------|-------|-----------------|
| `ppt` | Precipitation | mm | 1895 |
| `tmin` | Minimum Temperature | °C | 1895 |
| `tmax` | Maximum Temperature | °C | 1895 |
| `tmean` | Mean Temperature | °C | 1895 (derived) |
| `tdmean` | Mean Dew Point Temperature | °C | 1895 (estimated pre-1930s) |
| `vpdmin` | Minimum Vapor Pressure Deficit | hPa | 1895 (estimated pre-1930s) |
| `vpdmax` | Maximum Vapor Pressure Deficit | hPa | 1895 (estimated pre-1930s) |

**Note**: `tdmean`, `vpdmin`, and `vpdmax` data before the 1930s-1940s are statistically estimated from temperature and precipitation records.

## Access Methods

### 1. FTP Access (Anonymous)

**Server**: `prism.oregonstate.edu`
**Authentication**: None required (anonymous access)
**Web Browser Access**: https://data.prism.oregonstate.edu

### 2. Web Services API

**Base URL**: `https://services.nacse.org/prism/data/public/`
**Resolutions Available**: 4km (free), 800m (free as of March 2025)

## Data Formats

- **BIL (Band Interleaved by Line)**: Primary raster format with floating-point values
- **ASCII Grid**: Available for 30-year normals
- **COG (Cloud Optimized GeoTIFF)**: Newer format for cloud computing

## File Naming Convention

PRISM files follow a standardized naming pattern:

```
PRISM_[variable]_[stability]_[resolution]_[date]_[format]
```

Examples:
- `PRISM_ppt_stable_4kmM3_197101_bil` - January 1971 precipitation, monthly, 4km
- `PRISM_tmax_stable_4kmD2_20230615_bil` - June 15, 2023 max temp, daily, 4km
- `PRISM_tmean_30yr_normal_4kmM4_01_bil` - January 30-year normal mean temp

## Python Bulk Download Script

A comprehensive Python script (`prism_bulk_download.py`) is provided for bulk downloading.

### Installation

```bash
# Install required packages
pip install tqdm

# Make script executable (Linux/Mac)
chmod +x prism_bulk_download.py
```

### Usage Examples

#### Download Monthly Data from 1971-1980

```bash
python prism_bulk_download.py \
  --temporal monthly \
  --start 1971-01-01 \
  --end 1980-12-31 \
  --variables ppt tmin tmax \
  --output-dir ./prism_data
```

#### Download All Variables for 1971-2023 (Monthly)

```bash
python prism_bulk_download.py \
  --temporal monthly \
  --start 1971-01-01 \
  --end 2023-12-31 \
  --all-variables \
  --output-dir ./prism_data \
  --max-workers 4
```

#### Download Daily Data for 2020

```bash
python prism_bulk_download.py \
  --temporal daily \
  --start 2020-01-01 \
  --end 2020-12-31 \
  --variables ppt tmin tmax \
  --output-dir ./prism_data
```

### Script Features

- **Parallel Downloads**: Configurable number of concurrent downloads
- **Automatic Retry**: Failed downloads are retried with exponential backoff
- **Progress Tracking**: Visual progress bars for each file
- **Resume Capability**: Skips already downloaded files
- **Organized Storage**: Files organized by variable and temporal resolution
- **Logging**: Detailed logs saved to `prism_download.log`

## Alternative Download Methods

### Using wget

```bash
# Download monthly precipitation for January 1971
wget https://services.nacse.org/prism/data/public/4km/ppt/197101 -O PRISM_ppt_197101.zip

# Download with recursive options for FTP
wget -r -np -nH --cut-dirs=2 ftp://prism.oregonstate.edu/monthly/ppt/1971/
```

### Using curl

```bash
# Download specific file
curl -O https://services.nacse.org/prism/data/public/4km/ppt/197101

# Download with output filename
curl https://services.nacse.org/prism/data/public/4km/ppt/197101 -o prism_ppt_197101.zip
```

### Using Python urllib (Simple Example)

```python
import urllib.request
from datetime import datetime, timedelta

# Configuration
base_url = "https://services.nacse.org/prism/data/public/4km"
variable = "ppt"  # precipitation
start_date = datetime(1971, 1, 1)
end_date = datetime(1971, 12, 31)

# Download monthly data
current = start_date
while current <= end_date:
    date_str = current.strftime("%Y%m")
    url = f"{base_url}/{variable}/{date_str}"
    filename = f"PRISM_{variable}_{date_str}.zip"

    print(f"Downloading {filename}...")
    urllib.request.urlretrieve(url, filename)

    # Move to next month
    if current.month == 12:
        current = current.replace(year=current.year + 1, month=1)
    else:
        current = current.replace(month=current.month + 1)
```

## Data Processing Tips

### Extract BIL Files

```python
import zipfile
import os

def extract_prism_data(zip_path, extract_to="./extracted"):
    """Extract PRISM zip files"""
    os.makedirs(extract_to, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

    print(f"Extracted {zip_path} to {extract_to}")
```

### Read BIL Format

```python
import numpy as np
from osgeo import gdal

def read_bil_file(bil_path):
    """Read PRISM BIL file"""
    dataset = gdal.Open(bil_path)
    if dataset:
        array = dataset.ReadAsArray()
        geotransform = dataset.GetGeoTransform()
        projection = dataset.GetProjection()
        return array, geotransform, projection
    return None
```

## Important Considerations

### 1. Download Restrictions
- No authentication required for FTP access
- Be respectful of server resources
- Use reasonable parallelization (4-8 concurrent downloads recommended)
- No official rate limits documented, but avoid aggressive downloading

### 2. Data Storage Requirements
- Monthly data: ~10-50 MB per variable per year (compressed)
- Daily data: ~2-5 GB per variable per year (compressed)
- Plan storage accordingly for bulk downloads

### 3. Data Updates
- Daily data typically available with 1-day lag
- Monthly data available after month completion
- Check [PRISM Update Calendar](https://prism.oregonstate.edu/calendar) for schedules
- Data may be revised; version tracking important for reproducibility

### 4. Coordinate System
- Projection: Geographic (Lat/Long)
- Datum: NAD83
- Resolution: 4km (~0.04167 degrees) or 800m (~0.00833 degrees)

## Directory Structure After Download

```
prism_data/
├── ppt/
│   ├── monthly/
│   │   ├── PRISM_ppt_4km_monthly_197101.zip
│   │   ├── PRISM_ppt_4km_monthly_197102.zip
│   │   └── ...
│   └── daily/
│       └── ...
├── tmin/
│   ├── monthly/
│   └── daily/
├── tmax/
│   ├── monthly/
│   └── daily/
└── prism_download.log
```

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Increase timeout values or retry later
2. **404 Errors**: Check date ranges (daily only from 1981)
3. **Incomplete Downloads**: Use script's retry mechanism
4. **Memory Issues**: Process data in chunks for large datasets

### Contact Information

- **Email**: prism-questions@nacse.org
- **Website**: https://prism.oregonstate.edu/
- **Documentation**: https://prism.oregonstate.edu/documents/

## Additional Resources

- [PRISM Data Update Schedule](https://prism.oregonstate.edu/documents/PRISM_update_schedule.pdf)
- [PRISM Explorer](https://prism.oregonstate.edu/explorer/) - Interactive data viewer
- [pyPRISMClimate](https://github.com/sdtaylor/pyPRISMClimate) - Python package for PRISM data
- [R prism package](https://github.com/ropensci/prism) - R interface to PRISM data

## Citation

When using PRISM data, please cite:

```
PRISM Climate Group, Oregon State University, https://prism.oregonstate.edu,
data created [date], accessed [date].
```

---

*Last Updated: 2025-09-16*