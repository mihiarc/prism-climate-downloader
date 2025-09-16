#!/usr/bin/env python3
"""
Test script for non-temperature variables - downloads just 3 days of data to verify everything works
"""

import sys
sys.path.append('.')
from download_daily_other_1981_2000 import PRISMDailyOtherDownloader
from datetime import datetime

# Test with just 3 days from January 1981
downloader = PRISMDailyOtherDownloader(output_dir="./test_prism_other_download")

print("🧪 Testing PRISM non-temperature download with 3 days of data (1981-01-01 to 1981-01-03)")
print("This should download 12 files (3 days × 4 variables)")
print("-" * 50)

start = datetime(1981, 1, 1)
end = datetime(1981, 1, 3)

# Test with just precipitation first (smaller test)
print("\n📊 First testing with just precipitation (ppt) - 3 files")
results = downloader.download_range(start, end, variables=['ppt'], max_workers=2)

if any(r['success'] for r in results):
    print("\n✅ Precipitation test successful! Now testing all variables...")
    results_all = downloader.download_range(start, end, max_workers=2)
    print("\n✅ Test complete! If successful, you can run the full download.")
else:
    print("\n❌ Test failed. Please check your internet connection and try again.")