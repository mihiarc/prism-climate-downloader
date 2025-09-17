#!/usr/bin/env python3
"""
Test script for all variables downloader - downloads just 3 days of 2001 data
"""

import sys
sys.path.append('.')
from download_daily_all_2001_2024 import PRISMDailyAllVariablesDownloader
from datetime import datetime

# Test with just 3 days from January 2001
downloader = PRISMDailyAllVariablesDownloader(output_dir="./test_prism_all_2001")

print("🧪 Testing PRISM all variables download with 3 days of data (2001-01-01 to 2001-01-03)")
print("This should download 21 files (3 days × 7 variables)")
print("-" * 50)

start = datetime(2001, 1, 1)
end = datetime(2001, 1, 3)

# Test with all variables
results = downloader.download_range(start, end, max_workers=2)

print("\n✅ Test complete! If successful, you can run the full download script.")