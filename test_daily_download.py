#!/usr/bin/env python3
"""
Test script - downloads just 3 days of data to verify everything works
"""

import sys
sys.path.append('.')
from download_daily_temp_1981_2000 import PRISMDailyDownloader
from datetime import datetime

# Test with just 3 days from January 1981
downloader = PRISMDailyDownloader(output_dir="./test_prism_download")

print("🧪 Testing PRISM download with 3 days of data (1981-01-01 to 1981-01-03)")
print("This should download 9 files (3 days × 3 temperature variables)")
print("-" * 50)

start = datetime(1981, 1, 1)
end = datetime(1981, 1, 3)

results = downloader.download_range(start, end, max_workers=2)

print("\n✅ Test complete! If successful, you can run the full download.")