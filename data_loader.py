"""
TTR AI Workspace - Data Loader
Handles ETL and data transformations for DART P90 and TP90 analysis
T4W = Trailing 4 Weeks (Kingpin measurement)
Data filtered to 2026 only
"""

import pandas as pd
import numpy as np
from pathlib import Path
from config import APAC_SITES, DART_P90_STRETCH, DART_P90_CEILING, DATA_DIR

# TP90 Target (Overall - all cases)
TP90_TARGET = 9.69  # APAC Kingpin goal

def load_closed_deviation_data(filepath=None):
    """Load and process closed deviation data - 2026 only."""
    if filepath is None:
        filepath = Path(DATA_DIR) / "2026 Closed Deviation WW.csv"
    
    df = pd.read_csv(filepath)
    
    # Case classification
    has_escalation_outcome = 'escalation_outcome' in df.columns
    has_ttr_escalation = 'ttr_escalation' in df.columns
    
    if has_escalation_outcome and has_ttr_escalation:
        df['case_classification'] = np.where(
            df['escalation_outcome'].notna() | (df['ttr_escalation'].notna() & (df['ttr_escalation'] > 0)),
            'External Hold',
            'Internal Ops'
        )
    elif has_escalation_outcome:
        df['case_classification'] = np.where(
            df['escalation_outcome'].notna(),
            'External Hold',
            'Internal Ops'
        )
    else:
        df['case_classification'] = 'Internal Ops'
    
    # Date parsing
    df['dev_end_date_parsed'] = pd.to_datetime(df['dev_end_date'])
    df['month'] = df['dev_end_date_parsed'].dt.to_period('M').astype(str)
    df['year'] = df['dev_end_date_parsed'].dt.year
    
    # Calculate dev_end_week from dev_end_date (ISO week) — aligns with QuickSight
    df['year_week'] = 'WK' + df['dev_end_date_parsed'].dt.isocalendar().week.astype(str)
    
    # Filter to 2026 only
    df = df[df['year'] == 2026].copy()
    
    # Site normalization
    site_col = 'dev_end_site_name' if 'dev_end_site_name' in df.columns else 'site_name'
    if site_col in df.columns:
        df['site'] = df[site_col]
    else:
        df['site'] = 'Unknown'
    
    df['region'] = np.where(df['site'].isin(APAC_SITES), 'APAC', 'Non-APAC')
    
    return df


def get_internal_ops_by_sites(df, selected_sites):
    """Filter to Internal Ops cases for selected sites."""
    return df[
        (df['case_classification'] == 'Internal Ops') & 
        (df['site'].isin(selected_sites))
    ].copy()


def get_all_cases_by_sites(df, selected_sites):
    """Get ALL cases (Internal Ops + External Hold) for selected sites."""
    return df[df['site'].isin(selected_sites)].copy()



def get_t4w_weeks(df):
    """Get the last 4 weeks from the data, sorted numerically."""
    weeks = df['year_week'].unique()
    # Sort numerically by extracting week number (handles WK6, WK16, etc.)
    weeks_sorted = sorted(weeks, key=lambda x: int(x.replace('WK', '')))
    return weeks_sorted[-4:] if len(weeks_sorted) >= 4 else weeks_sorted
# =============================================================================
# DART P90 FUNCTIONS (Internal Ops Only)
# =============================================================================

def calculate_weekly_dart_p90(df, selected_sites=None):
    """Calculate weekly DART P90 metrics (Internal Ops only)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    internal_ops = get_internal_ops_by_sites(df, selected_sites)
    
    if len(internal_ops) == 0:
        return pd.DataFrame()
    
    weekly = internal_ops.groupby('year_week')['ttr_ops_overall'].agg([
        ('dart_p90', lambda x: round(x.quantile(0.9), 2)),
        ('dart_p75', lambda x: round(x.quantile(0.75), 2)),
        ('dart_median', lambda x: round(x.quantile(0.5), 2)),
        ('dart_mean', lambda x: round(x.mean(), 2)),
        ('case_count', 'count')
    ]).reset_index()
    
    weekly['status'] = weekly['dart_p90'].apply(
        lambda x: 'Green' if x <= DART_P90_STRETCH else ('Yellow' if x <= DART_P90_CEILING else 'Red')
    )
    
    weekly['stretch_target'] = DART_P90_STRETCH
    weekly['ceiling_target'] = DART_P90_CEILING
    
    # Sort by week number (WK1, WK2, ... WK16)
    weekly = weekly.sort_values('year_week', key=lambda x: x.str.replace('WK', '').astype(int))
    
    return weekly

def calculate_t4w_dart_p90(df, selected_sites=None):
    """Calculate T4W (Trailing 4 Weeks) DART P90."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    internal_ops = get_internal_ops_by_sites(df, selected_sites)
    
    if len(internal_ops) == 0:
        return {'t4w_dart_p90': 0, 't4w_weeks': [], 't4w_case_count': 0}
    
    t4w_weeks = get_t4w_weeks(internal_ops)
    t4w_data = internal_ops[internal_ops['year_week'].isin(t4w_weeks)]
    
    if len(t4w_data) == 0:
        return {'t4w_dart_p90': 0, 't4w_weeks': t4w_weeks, 't4w_case_count': 0}
    
    t4w_dart_p90 = round(t4w_data['ttr_ops_overall'].quantile(0.9), 2)
    
    return {
        't4w_dart_p90': t4w_dart_p90,
        't4w_weeks': t4w_weeks,
        't4w_case_count': len(t4w_data),
        't4w_status': 'Green' if t4w_dart_p90 <= DART_P90_STRETCH else ('Yellow' if t4w_dart_p90 <= DART_P90_CEILING else 'Red')
    }


def calculate_manager_dart_p90(df, selected_sites=None):
    """Calculate DART P90 by manager (Internal Ops only)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    internal_ops = get_internal_ops_by_sites(df, selected_sites)
    
    if len(internal_ops) == 0:
        return pd.DataFrame()
    
    sup_col = None
    for col in ['dev_end_supervisor_id', 'supervisor_id', 'manager']:
        if col in internal_ops.columns:
            sup_col = col
            break
    
    if sup_col is None:
        return pd.DataFrame()
    
    by_manager = internal_ops.groupby(['year_week', sup_col])['ttr_ops_overall'].agg([
        ('dart_p90', lambda x: round(x.quantile(0.9), 2)),
        ('dart_median', lambda x: round(x.quantile(0.5), 2)),
        ('case_count', 'count')
    ]).reset_index()
    
    by_manager.rename(columns={sup_col: 'dev_end_supervisor_id'}, inplace=True)
    
    by_manager['status'] = by_manager['dart_p90'].apply(
        lambda x: 'Green' if x <= DART_P90_STRETCH else ('Yellow' if x <= DART_P90_CEILING else 'Red')
    )
    
    return by_manager


def calculate_vertical_dart_p90(df, selected_sites=None):
    """Calculate DART P90 by vertical (Internal Ops only)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    internal_ops = get_internal_ops_by_sites(df, selected_sites)
    
    if len(internal_ops) == 0 or 'dev_reason' not in internal_ops.columns:
        return pd.DataFrame()
    
    by_vertical = internal_ops.groupby(['year_week', 'dev_reason'])['ttr_ops_overall'].agg([
        ('dart_p90', lambda x: round(x.quantile(0.9), 2)),
        ('dart_median', lambda x: round(x.quantile(0.5), 2)),
        ('case_count', 'count')
    ]).reset_index()
    
    by_vertical['status'] = by_vertical['dart_p90'].apply(
        lambda x: 'Green' if x <= DART_P90_STRETCH else ('Yellow' if x <= DART_P90_CEILING else 'Red')
    )
    
    return by_vertical


def get_summary_stats(df, selected_sites=None):
    """Get DART P90 summary statistics (Internal Ops only)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    internal_ops = get_internal_ops_by_sites(df, selected_sites)
    
    if len(internal_ops) == 0:
        return {
            'total_cases': 0, 'overall_p90': 0, 'overall_median': 0, 'overall_mean': 0,
            'latest_week': 'N/A', 'latest_dart_p90': 0, 'latest_status': 'N/A',
            'green_weeks': 0, 'yellow_weeks': 0, 'red_weeks': 0, 'total_weeks': 0,
            't4w_dart_p90': 0, 't4w_status': 'N/A', 't4w_case_count': 0
        }
    
    total_cases = len(internal_ops)
    overall_p90 = round(internal_ops['ttr_ops_overall'].quantile(0.9), 2)
    overall_median = round(internal_ops['ttr_ops_overall'].median(), 2)
    overall_mean = round(internal_ops['ttr_ops_overall'].mean(), 2)
    
    weekly = calculate_weekly_dart_p90(df, selected_sites)
    latest_week = weekly.iloc[-1] if len(weekly) > 0 else None
    
    green_weeks = (weekly['status'] == 'Green').sum() if len(weekly) > 0 else 0
    yellow_weeks = (weekly['status'] == 'Yellow').sum() if len(weekly) > 0 else 0
    red_weeks = (weekly['status'] == 'Red').sum() if len(weekly) > 0 else 0
    
    # T4W DART P90
    t4w_stats = calculate_t4w_dart_p90(df, selected_sites)
    
    return {
        'total_cases': total_cases,
        'overall_p90': overall_p90,
        'overall_median': overall_median,
        'overall_mean': overall_mean,
        'latest_week': latest_week['year_week'] if latest_week is not None else 'N/A',
        'latest_dart_p90': latest_week['dart_p90'] if latest_week is not None else 0,
        'latest_status': latest_week['status'] if latest_week is not None else 'N/A',
        'green_weeks': green_weeks,
        'yellow_weeks': yellow_weeks,
        'red_weeks': red_weeks,
        'total_weeks': len(weekly),
        't4w_dart_p90': t4w_stats['t4w_dart_p90'],
        't4w_status': t4w_stats.get('t4w_status', 'N/A'),
        't4w_case_count': t4w_stats['t4w_case_count'],
        't4w_weeks': t4w_stats.get('t4w_weeks', [])
    }


# =============================================================================
# TP90 FUNCTIONS (All Cases - Internal Ops + External Hold) - T4W
# =============================================================================

def calculate_weekly_tp90(df, selected_sites=None):
    """Calculate weekly TP90 metrics (ALL cases)."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    
    if len(all_cases) == 0:
        return pd.DataFrame()
    
    weekly = all_cases.groupby('year_week')['ttr_ops_overall'].agg([
        ('tp90', lambda x: round(x.quantile(0.9), 2)),
        ('tp75', lambda x: round(x.quantile(0.75), 2)),
        ('tp_median', lambda x: round(x.quantile(0.5), 2)),
        ('tp_mean', lambda x: round(x.mean(), 2)),
        ('total_cases', 'count')
    ]).reset_index()
    
    # Add case breakdown
    internal_counts = all_cases[all_cases['case_classification'] == 'Internal Ops'].groupby('year_week').size()
    external_counts = all_cases[all_cases['case_classification'] == 'External Hold'].groupby('year_week').size()
    
    weekly['internal_ops_count'] = weekly['year_week'].map(internal_counts).fillna(0).astype(int)
    weekly['external_hold_count'] = weekly['year_week'].map(external_counts).fillna(0).astype(int)
    weekly['external_hold_pct'] = round(weekly['external_hold_count'] / weekly['total_cases'] * 100, 1)
    
    # Status based on TP90 target
    weekly['status'] = weekly['tp90'].apply(
        lambda x: 'Green' if x <= TP90_TARGET else 'Red'
    )
    
    weekly['target'] = TP90_TARGET
    
    # Sort by week number (WK1, WK2, ... WK16)
    weekly = weekly.sort_values('year_week', key=lambda x: x.str.replace('WK', '').astype(int))
    
    return weekly

def calculate_t4w_tp90(df, selected_sites=None):
    """Calculate T4W (Trailing 4 Weeks) TP90 - KINGPIN MEASUREMENT."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    
    if len(all_cases) == 0:
        return {
            't4w_tp90': 0, 't4w_weeks': [], 't4w_case_count': 0,
            't4w_internal_count': 0, 't4w_external_count': 0, 't4w_external_pct': 0
        }
    
    t4w_weeks = get_t4w_weeks(all_cases)
    t4w_data = all_cases[all_cases['year_week'].isin(t4w_weeks)]
    
    if len(t4w_data) == 0:
        return {
            't4w_tp90': 0, 't4w_weeks': t4w_weeks, 't4w_case_count': 0,
            't4w_internal_count': 0, 't4w_external_count': 0, 't4w_external_pct': 0
        }
    
    t4w_tp90 = round(t4w_data['ttr_ops_overall'].quantile(0.9), 2)
    t4w_internal = (t4w_data['case_classification'] == 'Internal Ops').sum()
    t4w_external = (t4w_data['case_classification'] == 'External Hold').sum()
    t4w_external_pct = round(t4w_external / len(t4w_data) * 100, 1)
    
    return {
        't4w_tp90': t4w_tp90,
        't4w_weeks': t4w_weeks,
        't4w_case_count': len(t4w_data),
        't4w_internal_count': t4w_internal,
        't4w_external_count': t4w_external,
        't4w_external_pct': t4w_external_pct,
        't4w_status': 'Green' if t4w_tp90 <= TP90_TARGET else 'Red'
    }


def calculate_weekly_comparison(df, selected_sites=None):
    """Calculate weekly DART P90 vs TP90 comparison."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    dart_weekly = calculate_weekly_dart_p90(df, selected_sites)
    tp90_weekly = calculate_weekly_tp90(df, selected_sites)
    
    if len(dart_weekly) == 0 or len(tp90_weekly) == 0:
        return pd.DataFrame()
    
    comparison = dart_weekly[['year_week', 'dart_p90', 'case_count']].merge(
        tp90_weekly[['year_week', 'tp90', 'total_cases', 'internal_ops_count', 'external_hold_count', 'external_hold_pct']],
        on='year_week',
        how='outer'
    )
    
    comparison['gap'] = round(comparison['tp90'] - comparison['dart_p90'], 2)
    
    # Sort by week number (WK1, WK2, ... WK16)
    comparison = comparison.sort_values('year_week', key=lambda x: x.str.replace('WK', '').astype(int))
    
    return comparison


def get_tp90_summary_stats(df, selected_sites=None):
    """Get TP90 summary statistics (ALL cases) - T4W based."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    
    if len(all_cases) == 0:
        return {
            'total_cases': 0, 'overall_tp90': 0, 'overall_median': 0,
            'internal_ops_count': 0, 'external_hold_count': 0, 'external_hold_pct': 0,
            'latest_week': 'N/A', 'latest_tp90': 0, 'latest_status': 'N/A',
            'green_weeks': 0, 'red_weeks': 0, 'total_weeks': 0,
            't4w_tp90': 0, 't4w_status': 'N/A', 't4w_case_count': 0,
            't4w_weeks': [], 't4w_external_pct': 0
        }
    
    total_cases = len(all_cases)
    overall_tp90 = round(all_cases['ttr_ops_overall'].quantile(0.9), 2)
    overall_median = round(all_cases['ttr_ops_overall'].median(), 2)
    
    internal_ops_count = (all_cases['case_classification'] == 'Internal Ops').sum()
    external_hold_count = (all_cases['case_classification'] == 'External Hold').sum()
    external_hold_pct = round(external_hold_count / total_cases * 100, 1)
    
    weekly = calculate_weekly_tp90(df, selected_sites)
    latest_week = weekly.iloc[-1] if len(weekly) > 0 else None
    
    green_weeks = (weekly['status'] == 'Green').sum() if len(weekly) > 0 else 0
    red_weeks = (weekly['status'] == 'Red').sum() if len(weekly) > 0 else 0
    
    # T4W TP90 - KINGPIN
    t4w_stats = calculate_t4w_tp90(df, selected_sites)
    
    return {
        'total_cases': total_cases,
        'overall_tp90': overall_tp90,
        'overall_median': overall_median,
        'internal_ops_count': internal_ops_count,
        'external_hold_count': external_hold_count,
        'external_hold_pct': external_hold_pct,
        'latest_week': latest_week['year_week'] if latest_week is not None else 'N/A',
        'latest_tp90': latest_week['tp90'] if latest_week is not None else 0,
        'latest_status': latest_week['status'] if latest_week is not None else 'N/A',
        'green_weeks': green_weeks,
        'red_weeks': red_weeks,
        'total_weeks': len(weekly),
        't4w_tp90': t4w_stats['t4w_tp90'],
        't4w_status': t4w_stats.get('t4w_status', 'N/A'),
        't4w_case_count': t4w_stats['t4w_case_count'],
        't4w_weeks': t4w_stats.get('t4w_weeks', []),
        't4w_external_pct': t4w_stats.get('t4w_external_pct', 0),
        't4w_internal_count': t4w_stats.get('t4w_internal_count', 0),
        't4w_external_count': t4w_stats.get('t4w_external_count', 0)
    }


# =============================================================================
# AI CONTEXT
# =============================================================================

def get_data_context_for_ai(df, selected_sites=None):
    """Generate a summary context string for AI chat."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    dart_stats = get_summary_stats(df, selected_sites)
    tp90_stats = get_tp90_summary_stats(df, selected_sites)
    dart_weekly = calculate_weekly_dart_p90(df, selected_sites)
    
    t4w_weeks_str = ', '.join(tp90_stats.get('t4w_weeks', []))
    
    context = f"""
TTR AI Workspace - Data Context (2026 Only)

Selected Sites: {', '.join(selected_sites)}

TP90 (Kingpin Goal - All Cases - T4W):
- Definition: 90th percentile TTR for ALL cases (Internal Ops + External Hold)
- Measurement: T4W (Trailing 4 Weeks)
- Target: ≤{TP90_TARGET} days
- Current T4W TP90: {tp90_stats['t4w_tp90']} days ({tp90_stats['t4w_status']})
- T4W Period: {t4w_weeks_str}
- T4W Cases: {tp90_stats['t4w_case_count']:,}
- T4W External Hold %: {tp90_stats['t4w_external_pct']}%

DART P90 (Internal Ops Only - T4W):
- Definition: 90th percentile TTR for Internal Ops cases only
- Stretch Target: ≤{DART_P90_STRETCH} days (Green)
- Ceiling Target: ≤{DART_P90_CEILING} days (Yellow)
- Current T4W DART P90: {dart_stats['t4w_dart_p90']} days ({dart_stats['t4w_status']})
- T4W Internal Ops Cases: {dart_stats['t4w_case_count']:,}
- Green Weeks: {dart_stats['green_weeks']}/{dart_stats['total_weeks']}

Weekly DART P90 Trend (2026):
"""
    for _, row in dart_weekly.iterrows():
        context += f"- {row['year_week']}: {row['dart_p90']} days ({row['status']}) - {row['case_count']} cases\n"
    
    return context
