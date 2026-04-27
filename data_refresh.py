"""
TTR AI Workspace - Data Refresh Module
Automatically downloads latest data from DataCentral ETL job

ETL Job: [Data Feed] 2026 AHA Ops T4W TP90 TTR
Job ID: 28338906
Profile ID: 13358481
Database: spars002

Download URL Pattern:
https://datacentral.a2z.com/servlet/results?job_run_id={JOB_RUN_ID}&encoding=UTF8&mimeType=plain

Job History Page:
https://datacentral.a2z.com/dw-platform/servlet/dwp/template/ConversionJobHistory.vm/job_id/28338906

Authentication: Uses Midway cookie file (~/.midway/cookie) from mwinit
"""

import os
import re
import subprocess
import requests
from pathlib import Path
from datetime import datetime
import pandas as pd
from io import StringIO

# Configuration
DATACENTRAL_JOB_ID = 28338906
DATACENTRAL_PROFILE_ID = 13358481
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "2026 Closed Deviation WW.csv"
MIDWAY_COOKIE_PATH = Path.home() / ".midway" / "cookie"

# DataCentral URLs
DATACENTRAL_BASE = "https://datacentral.a2z.com"
JOB_HISTORY_URL = f"{DATACENTRAL_BASE}/dw-platform/servlet/dwp/template/ConversionJobHistory.vm/job_id/{DATACENTRAL_JOB_ID}"

# Known recent job run IDs (fallback if auto-fetch fails)
KNOWN_JOB_RUNS = [
    12211116914,  # 2026-04-21 (latest)
    12201128628,  # 2026-04-18
    12196522278,  # 2026-04-17
    12195942706,  # 2026-04-16
    12195942704,  # 2026-04-15
]


def get_download_url(job_run_id):
    """Get the direct download URL for a job run."""
    return f"{DATACENTRAL_BASE}/servlet/results?job_run_id={job_run_id}&encoding=UTF8&mimeType=plain"


def download_with_curl(url, timeout=300):
    """
    Download data using curl with Midway cookie file authentication.
    This is the most reliable method for Amazon internal sites.
    """
    if not MIDWAY_COOKIE_PATH.exists():
        print(f"Midway cookie file not found at {MIDWAY_COOKIE_PATH}")
        print("Run 'mwinit' in terminal to authenticate")
        return None
    
    try:
        cmd = [
            'curl',
            '-s',           # Silent
            '-L',           # Follow redirects
            '-b', str(MIDWAY_COOKIE_PATH),  # Use Midway cookie file
            '--max-time', str(timeout),
            url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
        
        if result.returncode == 0:
            return result.stdout
        else:
            print(f"curl failed (exit {result.returncode})")
            if result.stderr:
                print(f"stderr: {result.stderr[:200]}")
            return None
            
    except subprocess.TimeoutExpired:
        print("curl timeout - try again or use manual upload")
        return None
    except FileNotFoundError:
        print("curl not found - please install curl")
        return None
    except Exception as e:
        print(f"curl error: {e}")
        return None


def fetch_latest_job_run_id():
    """
    Fetch the latest job run ID from DataCentral job history page.
    Scrapes the HTML to find job_run_id values.
    """
    print("Fetching latest job run ID from DataCentral...")
    
    html = download_with_curl(JOB_HISTORY_URL, timeout=30)
    
    if not html:
        print("Could not fetch job history page")
        return None
    
    # Check if we got a login page instead
    if 'midway' in html.lower()[:500] or 'login' in html.lower()[:500]:
        print("Got login page - Midway session may be expired")
        return None
    
    # Look for job_run_id patterns in the HTML
    pattern = r'job_run_id[=:](\d{10,12})'
    matches = re.findall(pattern, html)
    
    if matches:
        job_run_ids = [int(m) for m in matches]
        latest = max(job_run_ids)
        print(f"Found latest job run ID: {latest}")
        return latest
    
    # Fallback: look for 11-12 digit numbers starting with 12
    pattern2 = r'\b(12\d{9,10})\b'
    matches = re.findall(pattern2, html)
    
    if matches:
        job_run_ids = [int(m) for m in matches]
        latest = max(job_run_ids)
        print(f"Found job run ID via pattern: {latest}")
        return latest
    
    print("Could not find job run ID in page")
    return None


def download_data(job_run_id):
    """
    Download data from DataCentral for a specific job run.
    Returns DataFrame or None.
    """
    url = get_download_url(job_run_id)
    print(f"Downloading job run {job_run_id}...")
    
    content = download_with_curl(url, timeout=300)
    
    if not content:
        return None
    
    # Check if we got actual data
    content_lower = content[:1000].lower()
    data_indicators = ['tp90_ttr', 'ttr_overall', 'dev_id', 'site_name', 'reporting_week']
    
    if any(ind in content_lower for ind in data_indicators):
        # Detect separator
        first_line = content.split('\n')[0]
        sep = '\t' if '\t' in first_line else ','
        
        df = pd.read_csv(StringIO(content), sep=sep)
        print(f"Downloaded {len(df):,} rows")
        return df
    
    # Check for login page
    if 'midway' in content_lower or 'login' in content_lower:
        print("Got login page - run 'mwinit' to refresh session")
    else:
        print("Response didn't contain expected data columns")
    
    return None


def filter_apac_2026(df):
    """Filter data to APAC sites (SIN + HND) and 2026 only."""
    if df is None or len(df) == 0:
        return df
    
    original = len(df)
    
    # Find site column
    for col in ['site_name', 'dev_end_site_name', 'reporting_site']:
        if col in df.columns:
            df = df[df[col].isin(['SIN', 'HND'])].copy()
            print(f"Filtered by site: {original:,} → {len(df):,}")
            break
    
    # Find year column
    for col in ['reporting_year', 'dev_created_year']:
        if col in df.columns:
            before = len(df)
            df = df[df[col] >= 2026].copy()
            print(f"Filtered by year: {before:,} → {len(df):,}")
            break
    
    return df


def get_date_range(df):
    """Get min/max dates from dataframe."""
    for col in ['dev_end_date', 'end_date_range', 'reporting_date']:
        if col in df.columns:
            try:
                dates = pd.to_datetime(df[col], errors='coerce')
                return dates.min().strftime('%Y-%m-%d'), dates.max().strftime('%Y-%m-%d')
            except:
                pass
    return None, None


def refresh_data(job_run_id=None):
    """
    Main function to refresh data from DataCentral.
    
    Returns dict with: success, message, rows, job_run_id
    """
    result = {
        'success': False,
        'message': '',
        'rows': 0,
        'job_run_id': None
    }
    
    # Check Midway cookie exists
    print("\n" + "=" * 50)
    print("Checking authentication...")
    print("=" * 50)
    
    if not MIDWAY_COOKIE_PATH.exists():
        result['message'] = (
            "Midway cookie not found.\n\n"
            "Run `mwinit` in terminal to authenticate, then try again."
        )
        return result
    
    print(f"✓ Found Midway cookie: {MIDWAY_COOKIE_PATH}")
    
    # Build list of job runs to try
    job_runs_to_try = []
    
    if job_run_id:
        job_runs_to_try.append(job_run_id)
    else:
        # Try to auto-fetch latest
        print("\n" + "=" * 50)
        print("Finding latest job run...")
        print("=" * 50)
        
        latest = fetch_latest_job_run_id()
        if latest:
            job_runs_to_try.append(latest)
        
        # Add known fallbacks
        job_runs_to_try.extend(KNOWN_JOB_RUNS)
        
        # Remove duplicates
        seen = set()
        job_runs_to_try = [x for x in job_runs_to_try if x and not (x in seen or seen.add(x))]
    
    # Try downloading
    print("\n" + "=" * 50)
    print("Downloading data...")
    print("=" * 50)
    
    df = None
    for run_id in job_runs_to_try:
        df = download_data(run_id)
        if df is not None and len(df) > 0:
            result['job_run_id'] = run_id
            break
        print(f"Job run {run_id} failed, trying next...")
    
    if df is None or len(df) == 0:
        result['message'] = (
            "Could not download data.\n\n"
            "Try:\n"
            "1. Run `mwinit` to refresh Midway session\n"
            "2. Check VPN connection\n"
            "3. Use manual upload"
        )
        return result
    
    # Filter
    print("\n" + "=" * 50)
    print("Filtering to APAC 2026...")
    print("=" * 50)
    
    df_filtered = filter_apac_2026(df)
    
    if df_filtered is None or len(df_filtered) == 0:
        result['message'] = "Downloaded data but no APAC 2026 records found."
        return result
    
    # Save
    print("\n" + "=" * 50)
    print("Saving data...")
    print("=" * 50)
    
    DATA_DIR.mkdir(exist_ok=True)
    df_filtered.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to: {OUTPUT_FILE}")
    
    # Success
    result['success'] = True
    result['rows'] = len(df_filtered)
    
    min_date, max_date = get_date_range(df_filtered)
    if min_date and max_date:
        result['message'] = (
            f"✅ Refreshed successfully!\n\n"
            f"• **{len(df_filtered):,}** APAC rows\n"
            f"• Data: {min_date} → {max_date}\n"
            f"• Job run: {result['job_run_id']}"
        )
    else:
        result['message'] = f"✅ Refreshed: {len(df_filtered):,} APAC rows"
    
    return result


def manual_upload(file_content, filename="uploaded.csv"):
    """Process a manually uploaded file."""
    result = {'success': False, 'message': '', 'rows': 0}
    
    try:
        if isinstance(file_content, bytes):
            try:
                file_content = file_content.decode('utf-8')
            except:
                file_content = file_content.decode('latin-1')
        
        sep = '\t' if '\t' in file_content.split('\n')[0] else ','
        df = pd.read_csv(StringIO(file_content), sep=sep)
        
        df_filtered = filter_apac_2026(df)
        
        if len(df_filtered) == 0:
            result['message'] = "No APAC 2026 records found"
            return result
        
        DATA_DIR.mkdir(exist_ok=True)
        df_filtered.to_csv(OUTPUT_FILE, index=False)
        
        result['success'] = True
        result['rows'] = len(df_filtered)
        
        min_date, max_date = get_date_range(df_filtered)
        if min_date:
            result['message'] = f"✅ Uploaded {len(df_filtered):,} rows ({min_date} → {max_date})"
        else:
            result['message'] = f"✅ Uploaded {len(df_filtered):,} rows"
        
        return result
        
    except Exception as e:
        result['message'] = f"Error: {e}"
        return result


if __name__ == "__main__":
    print("=" * 60)
    print("TTR AI Workspace - Data Refresh")
    print("=" * 60)
    print(f"Job ID: {DATACENTRAL_JOB_ID}")
    print(f"Output: {OUTPUT_FILE}")
    
    result = refresh_data()
    
    print("\n" + "=" * 60)
    print("RESULT")
    print("=" * 60)
    print(result['message'])
    
    if not result['success']:
        print(f"\nManual refresh:")
        print(f"1. Open: {JOB_HISTORY_URL}")
        print(f"2. Download latest job run as TSV/CSV")
        print(f"3. Upload via app sidebar")
