"""
Open Deviation Data Loader
Handles loading and processing of open deviation data for alerts and monitoring.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from config import DATA_DIR, APAC_SITES

# Alert thresholds (in days)
ALERT_HEALTHY = 7
ALERT_WATCH = 11
ALERT_WARNING = 14  # 12-14 days
ALERT_CRITICAL = 20
ALERT_SEVERE = 21

def load_open_deviation_data(filepath=None):
    """Load and process open deviation data."""
    if filepath is None:
        filepath = Path(DATA_DIR) / "2026 Open Deviation WW.csv"
    
    if not Path(filepath).exists():
        # Try parent TTR folder
        filepath = Path(r"C:\Users\dangjodi\Desktop\AHA\TTR\2026 Open Deviation WW.csv")
    
    if not Path(filepath).exists():
        return None
    
    df = pd.read_csv(filepath)
    
    # Standardize column names
    df.columns = df.columns.str.strip()
    
    # Ensure Ageing Days is numeric
    if 'Ageing Days' in df.columns:
        df['ageing_days'] = pd.to_numeric(df['Ageing Days'], errors='coerce').fillna(0)
    elif 'ageing_days' in df.columns:
        df['ageing_days'] = pd.to_numeric(df['ageing_days'], errors='coerce').fillna(0)
    else:
        df['ageing_days'] = 0
    
    # Add alert level
    df['alert_level'] = df['ageing_days'].apply(get_alert_level)
    df['alert_color'] = df['alert_level'].map({
        'Healthy': '#C8E6C9',
        'Watch': '#FFF9C4', 
        'Warning': '#FFE0B2',
        'Critical': '#FFCDD2',
        'Severe': '#F44336'
    })
    
    return df


def get_alert_level(days):
    """Determine alert level based on ageing days."""
    if days <= ALERT_HEALTHY:
        return 'Healthy'
    elif days <= ALERT_WATCH:
        return 'Watch'
    elif days <= ALERT_WARNING:
        return 'Warning'
    elif days <= ALERT_CRITICAL:
        return 'Critical'
    else:
        return 'Severe'


def get_open_deviation_by_sites(df, selected_sites=None):
    """Filter open deviations to selected sites."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    return df[df['site_name'].isin(selected_sites)].copy()


def get_open_deviation_summary(df, selected_sites=None):
    """Get summary statistics for open deviations."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    filtered = get_open_deviation_by_sites(df, selected_sites)
    
    if len(filtered) == 0:
        return {
            'total': 0,
            'healthy': 0,
            'watch': 0,
            'warning': 0,
            'critical': 0,
            'severe': 0,
            'alert_count': 0,
            'avg_ageing': 0,
            'max_ageing': 0
        }
    
    alert_counts = filtered['alert_level'].value_counts()
    
    return {
        'total': len(filtered),
        'healthy': alert_counts.get('Healthy', 0),
        'watch': alert_counts.get('Watch', 0),
        'warning': alert_counts.get('Warning', 0),
        'critical': alert_counts.get('Critical', 0),
        'severe': alert_counts.get('Severe', 0),
        'alert_count': alert_counts.get('Warning', 0) + alert_counts.get('Critical', 0) + alert_counts.get('Severe', 0),
        'avg_ageing': round(filtered['ageing_days'].mean(), 1),
        'max_ageing': int(filtered['ageing_days'].max())
    }


def get_open_by_stage(df, selected_sites=None):
    """Get open deviations grouped by ops stage."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    filtered = get_open_deviation_by_sites(df, selected_sites)
    
    if len(filtered) == 0 or 'ttr_stage' not in filtered.columns:
        return pd.DataFrame()
    
    by_stage = filtered.groupby('ttr_stage').agg({
        'ageing_days': ['count', 'mean', 'max'],
        'dev_id': 'nunique'
    }).round(2)
    by_stage.columns = ['count', 'avg_days', 'max_days', 'unique_cases']
    by_stage = by_stage.sort_values('count', ascending=False)
    
    return by_stage.reset_index()


def get_open_by_vertical(df, selected_sites=None):
    """Get open deviations grouped by vertical (dev_reason)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    filtered = get_open_deviation_by_sites(df, selected_sites)
    
    if len(filtered) == 0:
        return pd.DataFrame()
    
    by_vertical = filtered.groupby('dev_reason').agg({
        'ageing_days': ['count', 'mean', 'max'],
        'dev_id': 'nunique'
    }).round(2)
    by_vertical.columns = ['count', 'avg_days', 'max_days', 'unique_cases']
    by_vertical = by_vertical.sort_values('count', ascending=False)
    
    return by_vertical.reset_index()


def get_open_by_manager(df, selected_sites=None):
    """Get open deviations grouped by supervisor."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    filtered = get_open_deviation_by_sites(df, selected_sites)
    
    if len(filtered) == 0 or 'supervisor_id' not in filtered.columns:
        return pd.DataFrame()
    
    by_manager = filtered.groupby('supervisor_id').agg({
        'ageing_days': ['count', 'mean', 'max'],
        'dev_id': 'nunique'
    }).round(2)
    by_manager.columns = ['count', 'avg_days', 'max_days', 'unique_cases']
    by_manager = by_manager.sort_values('max_days', ascending=False)
    
    return by_manager.reset_index()


def get_alert_cases(df, selected_sites=None, min_days=12):
    """Get cases that have triggered alerts (>=12 days by default)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    filtered = get_open_deviation_by_sites(df, selected_sites)
    
    alert_cases = filtered[filtered['ageing_days'] >= min_days].copy()
    alert_cases = alert_cases.sort_values('ageing_days', ascending=False)
    
    return alert_cases


def get_ageing_distribution(df, selected_sites=None):
    """Get distribution of cases by ageing bucket."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    filtered = get_open_deviation_by_sites(df, selected_sites)
    
    if len(filtered) == 0:
        return pd.DataFrame()
    
    # Create buckets
    bins = [0, 5, 7, 11, 14, 20, 30, 1000]
    labels = ['0-5d', '6-7d', '8-11d', '12-14d', '15-20d', '21-30d', '30+d']
    
    filtered['bucket'] = pd.cut(filtered['ageing_days'], bins=bins, labels=labels)
    
    distribution = filtered['bucket'].value_counts().sort_index()
    
    return pd.DataFrame({
        'Bucket': distribution.index,
        'Count': distribution.values,
        'Pct': (distribution.values / len(filtered) * 100).round(1)
    })
