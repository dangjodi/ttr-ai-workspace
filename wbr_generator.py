"""
TTR AI Workspace - WBR Generator
Auto-generates Weekly Business Review call-out narratives
Replaces Headcount 2: Closed Deviation manual analysis

KEY FEATURE: Tracks "Already Dived" cases to avoid re-investigating
recurring T4W outliers that were already analyzed in prior weeks.
"""

import pandas as pd
import numpy as np
from data_loader import (
    get_all_cases_by_sites, 
    get_internal_ops_by_sites,
    calculate_weekly_tp90,
    calculate_weekly_dart_p90,
    TP90_TARGET,
    APAC_SITES
)
from config import DART_P90_STRETCH, DART_P90_CEILING


def get_weekly_stats(df, week, selected_sites=None):
    """Get detailed stats for a specific week."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    week_cases = all_cases[all_cases['year_week'] == week]
    
    if len(week_cases) == 0:
        return None
    
    internal = week_cases[week_cases['case_classification'] == 'Internal Ops']
    external = week_cases[week_cases['case_classification'] == 'External Hold']
    
    return {
        'week': week,
        'total_cases': len(week_cases),
        'internal_count': len(internal),
        'external_count': len(external),
        'external_pct': round(len(external) / len(week_cases) * 100, 1) if len(week_cases) > 0 else 0,
        'tp90': round(week_cases['ttr_ops_overall'].quantile(0.9), 2),
        'tp75': round(week_cases['ttr_ops_overall'].quantile(0.75), 2),
        'avg_ttr': round(week_cases['ttr_ops_overall'].mean(), 2),
        'median_ttr': round(week_cases['ttr_ops_overall'].median(), 2),
        'dart_p90': round(internal['ttr_ops_overall'].quantile(0.9), 2) if len(internal) > 0 else 0,
        'max_ttr': round(week_cases['ttr_ops_overall'].max(), 2),
        'min_ttr': round(week_cases['ttr_ops_overall'].min(), 2)
    }


def get_wow_comparison(df, current_week, selected_sites=None):
    """Get week-over-week comparison."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    weeks = sorted(all_cases['year_week'].unique())
    
    if current_week not in weeks:
        return None
    
    current_idx = weeks.index(current_week)
    if current_idx == 0:
        return None
    
    prev_week = weeks[current_idx - 1]
    
    current_stats = get_weekly_stats(df, current_week, selected_sites)
    prev_stats = get_weekly_stats(df, prev_week, selected_sites)
    
    if not current_stats or not prev_stats:
        return None
    
    return {
        'current': current_stats,
        'previous': prev_stats,
        'tp90_change': round(current_stats['tp90'] - prev_stats['tp90'], 2),
        'avg_ttr_change': round(current_stats['avg_ttr'] - prev_stats['avg_ttr'], 2),
        'dart_p90_change': round(current_stats['dart_p90'] - prev_stats['dart_p90'], 2),
        'external_pct_change': round(current_stats['external_pct'] - prev_stats['external_pct'], 1),
        'case_count_change': current_stats['total_cases'] - prev_stats['total_cases']
    }


def get_t4w_weeks(df, current_week, selected_sites=None):
    """Get the T4W weeks ending at current_week."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    weeks = sorted(all_cases['year_week'].unique())
    
    if current_week not in weeks:
        return []
    
    current_idx = weeks.index(current_week)
    start_idx = max(0, current_idx - 3)  # Get 4 weeks including current
    
    return weeks[start_idx:current_idx + 1]


def get_t4w_stats(df, current_week=None, selected_sites=None):
    """Get T4W (Trailing 4 Weeks) statistics ending at current_week."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    weeks = sorted(all_cases['year_week'].unique())
    
    if current_week is None:
        current_week = weeks[-1] if weeks else None
    
    if current_week is None:
        return None
    
    t4w_weeks = get_t4w_weeks(df, current_week, selected_sites)
    
    if not t4w_weeks:
        return None
    
    t4w_cases = all_cases[all_cases['year_week'].isin(t4w_weeks)]
    
    if len(t4w_cases) == 0:
        return None
    
    internal = t4w_cases[t4w_cases['case_classification'] == 'Internal Ops']
    
    return {
        'weeks': t4w_weeks,
        'week_range': f"{t4w_weeks[0]} → {t4w_weeks[-1]}",
        'latest_week': t4w_weeks[-1],
        'total_cases': len(t4w_cases),
        'tp90': round(t4w_cases['ttr_ops_overall'].quantile(0.9), 2),
        'avg_ttr': round(t4w_cases['ttr_ops_overall'].mean(), 2),
        'dart_p90': round(internal['ttr_ops_overall'].quantile(0.9), 2) if len(internal) > 0 else 0,
        'external_pct': round((t4w_cases['case_classification'] == 'External Hold').sum() / len(t4w_cases) * 100, 1)
    }


def get_outliers(df, week=None, top_n=10, selected_sites=None):
    """
    Get P90+ outliers (bottom 10th percentile - longest aging cases).
    If week is None, uses T4W data. Deduplicated by dev_id.
    """
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    
    if week:
        # T4W ending at specified week
        t4w_weeks = get_t4w_weeks(df, week, selected_sites)
        cases = all_cases[all_cases['year_week'].isin(t4w_weeks)]
    else:
        # Latest T4W - sort numerically
        weeks = sorted(all_cases['year_week'].unique(), key=lambda x: int(x.replace('WK', '')))
        t4w_weeks = weeks[-4:] if len(weeks) >= 4 else weeks
        cases = all_cases[all_cases['year_week'].isin(t4w_weeks)]
    
    if len(cases) == 0:
        return pd.DataFrame()
    
    # Deduplicate by dev_id, keeping the row with highest ttr_overall
    cases = cases.sort_values('ttr_ops_overall', ascending=False)
    cases = cases.drop_duplicates(subset=['dev_id'], keep='first')
    
    # P90 threshold
    p90_threshold = cases['ttr_ops_overall'].quantile(0.9)
    
    # Get cases above P90 (the outliers)
    outliers = cases[cases['ttr_ops_overall'] >= p90_threshold].copy()
    
    # Sort by ttr_overall descending (longest first)
    outliers = outliers.sort_values('ttr_ops_overall', ascending=False)
    
    # Take top N
    outliers = outliers.head(top_n)
    
    # Add useful columns
    outliers['is_external'] = outliers['case_classification'] == 'External Hold'
    
    return outliers


def get_outliers_with_new_flag(df, current_week, top_n=50, selected_sites=None):
    """
    Get T4W outliers with a flag indicating if they are NEW this week.
    
    This solves the re-investigation problem:
    - 🆕 NEW = Case closed in current_week (needs investigation)
    - 🔄 RECURRING = Case closed in prior weeks (already dived last week)
    
    Returns ALL outliers (up to top_n), deduplicated by dev_id.
    """
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    
    # Sort weeks numerically
    weeks = sorted(all_cases['year_week'].unique(), key=lambda x: int(x.replace('WK', '')))
    
    if current_week not in weeks:
        return pd.DataFrame(), pd.DataFrame()
    
    # Get T4W weeks for current week
    t4w_weeks = get_t4w_weeks(df, current_week, selected_sites)
    
    if not t4w_weeks:
        return pd.DataFrame(), pd.DataFrame()
    
    # T4W cases - deduplicate by dev_id, keeping the row with highest ttr_overall
    t4w_cases = all_cases[all_cases['year_week'].isin(t4w_weeks)].copy()
    t4w_cases = t4w_cases.sort_values('ttr_ops_overall', ascending=False)
    t4w_cases = t4w_cases.drop_duplicates(subset=['dev_id'], keep='first')
    
    if len(t4w_cases) == 0:
        return pd.DataFrame(), pd.DataFrame()
    
    # P90 threshold for T4W
    p90_threshold = t4w_cases['ttr_ops_overall'].quantile(0.9)
    
    # All T4W outliers (already deduplicated)
    all_outliers = t4w_cases[t4w_cases['ttr_ops_overall'] >= p90_threshold].copy()
    all_outliers = all_outliers.sort_values('ttr_ops_overall', ascending=False)
    
    # Separate NEW vs RECURRING
    # NEW = closed in current_week
    # RECURRING = closed in prior T4W weeks (already appeared in last week's T4W)
    
    new_outliers = all_outliers[all_outliers['year_week'] == current_week].copy()
    recurring_outliers = all_outliers[all_outliers['year_week'] != current_week].copy()
    
    # Add flags
    new_outliers['outlier_status'] = '🆕 NEW'
    recurring_outliers['outlier_status'] = '🔄 RECURRING'
    
    # Add week closed for clarity
    new_outliers['closed_week'] = new_outliers['year_week']
    recurring_outliers['closed_week'] = recurring_outliers['year_week']
    
    # Return all (up to top_n each)
    return new_outliers.head(top_n), recurring_outliers.head(top_n)


def analyze_outlier_root_causes(outliers_df):
    """Analyze and categorize outlier root causes."""
    if len(outliers_df) == 0:
        return {
            'classification': {},
            'verticals': {},
            'top_verticals': [],
            'sites': {},
            'patterns': [],
            'total_outliers': 0,
            'avg_outlier_ttr': 0,
            'max_outlier_ttr': 0
        }
    
    # Count by classification
    classification_counts = outliers_df['case_classification'].value_counts().to_dict()
    
    # Count by vertical (dev_reason)
    vertical_counts = {}
    if 'dev_reason' in outliers_df.columns:
        vertical_counts = outliers_df['dev_reason'].value_counts().to_dict()
    
    # Count by site
    site_counts = {}
    if 'site' in outliers_df.columns:
        site_counts = outliers_df['site'].value_counts().to_dict()
    
    # Identify patterns
    patterns = []
    
    external_count = classification_counts.get('External Hold', 0)
    internal_count = classification_counts.get('Internal Ops', 0)
    
    if external_count > 0:
        patterns.append(f"External Hold (Policy Team delays): {external_count} cases")
    
    if internal_count > 0:
        patterns.append(f"Operational Controllable: {internal_count} cases")
    
    # Top verticals
    top_verticals = sorted(vertical_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    
    return {
        'classification': classification_counts,
        'verticals': vertical_counts,
        'top_verticals': top_verticals,
        'sites': site_counts,
        'patterns': patterns,
        'total_outliers': len(outliers_df),
        'avg_outlier_ttr': round(outliers_df['ttr_ops_overall'].mean(), 2),
        'max_outlier_ttr': round(outliers_df['ttr_ops_overall'].max(), 2)
    }


def generate_wbr_narrative(df, week, selected_sites=None):
    """
    Generate WBR call-out narrative for a specific week.
    This replaces HC2's manual analysis and writing.
    
    KEY IMPROVEMENT: Separates NEW vs RECURRING outliers so you don't
    re-investigate cases you already dived into last week.
    
    Shows ALL outliers in the narrative (not limited to 5).
    """
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    # Get all required data
    wow = get_wow_comparison(df, week, selected_sites)
    t4w = get_t4w_stats(df, week, selected_sites)
    
    # Get NEW vs RECURRING outliers (all of them)
    new_outliers, recurring_outliers = get_outliers_with_new_flag(df, week, top_n=50, selected_sites=selected_sites)
    
    new_outlier_analysis = analyze_outlier_root_causes(new_outliers)
    recurring_outlier_analysis = analyze_outlier_root_causes(recurring_outliers)
    
    # Combined analysis for overall stats
    all_outliers = pd.concat([new_outliers, recurring_outliers]) if len(new_outliers) > 0 or len(recurring_outliers) > 0 else pd.DataFrame()
    total_outlier_analysis = analyze_outlier_root_causes(all_outliers)
    
    if not wow:
        return "Insufficient data to generate WBR narrative."
    
    current = wow['current']
    
    # Build narrative
    narrative = []
    
    # === HEADER ===
    narrative.append(f"## 📊 Closed Deviation - {week}\n")
    
    # === T4W SECTION ===
    if t4w:
        t4w_status = "✅" if t4w['tp90'] <= TP90_TARGET else "⚠️"
        narrative.append(f"### T4W ({t4w['week_range']})")
        narrative.append(f"**T4W TP90: {t4w['tp90']} days** (Target: {TP90_TARGET}d) {t4w_status}")
        narrative.append(f"- T4W DART P90: {t4w['dart_p90']} days")
        narrative.append(f"- T4W Cases: {t4w['total_cases']:,}")
        narrative.append(f"- External Hold: {t4w['external_pct']}%\n")
        
        # T4W outlier summary
        if total_outlier_analysis['total_outliers'] > 0:
            narrative.append(f"**T4W Outlier Summary:** {total_outlier_analysis['total_outliers']} P90+ cases")
            narrative.append(f"- 🆕 **NEW this week:** {len(new_outliers)} (need investigation)")
            narrative.append(f"- 🔄 **Recurring from prior weeks:** {len(recurring_outliers)} (already dived)")
            narrative.append(f"- Longest aging: {total_outlier_analysis['max_outlier_ttr']} days")
            narrative.append("")
    
    # === WEEKLY SECTION ===
    tp90_direction = "📈" if wow['tp90_change'] > 0 else "📉" if wow['tp90_change'] < 0 else "➡️"
    avg_direction = "📈" if wow['avg_ttr_change'] > 0 else "📉" if wow['avg_ttr_change'] < 0 else "➡️"
    
    tp90_wow = f"+{wow['tp90_change']}" if wow['tp90_change'] > 0 else str(wow['tp90_change'])
    avg_wow = f"+{wow['avg_ttr_change']}" if wow['avg_ttr_change'] > 0 else str(wow['avg_ttr_change'])
    
    narrative.append(f"### {week} Performance")
    narrative.append(f"**TP90: {current['tp90']} days** ({tp90_wow} WoW) {tp90_direction}")
    narrative.append(f"**Average TTR: {current['avg_ttr']} days** ({avg_wow} WoW) {avg_direction}")
    narrative.append(f"- Cases Closed: {current['total_cases']}")
    narrative.append(f"- DART P90 (Internal Ops): {current['dart_p90']} days")
    narrative.append(f"- External Hold: {current['external_pct']}% ({current['external_count']} cases)\n")
    
    # === KEY DRIVERS ===
    narrative.append("### 📈 Key Drivers")
    
    if wow['tp90_change'] < 0:
        narrative.append(f"TP90 **improved** by {abs(wow['tp90_change'])} days WoW.")
        if new_outlier_analysis['top_verticals']:
            top_v = new_outlier_analysis['top_verticals'][0][0]
            narrative.append(f"- Improvement driven by expedited closure in {top_v} vertical")
        if wow['external_pct_change'] < 0:
            narrative.append(f"- External Hold % decreased by {abs(wow['external_pct_change'])}%")
    elif wow['tp90_change'] > 0:
        narrative.append(f"TP90 **increased** by {wow['tp90_change']} days WoW.")
        if new_outlier_analysis['classification'].get('External Hold', 0) > 0:
            narrative.append(f"- External policy team review dependencies created delays")
        if new_outlier_analysis['top_verticals']:
            top_v = new_outlier_analysis['top_verticals'][0][0]
            narrative.append(f"- Primary impact from {top_v} vertical")
    else:
        narrative.append("TP90 remained **stable** WoW.")
    
    narrative.append("")
    
    # === NEW OUTLIERS (Need Investigation) - SHOW ALL ===
    if len(new_outliers) > 0:
        narrative.append(f"### 🆕 NEW Outliers This Week ({len(new_outliers)}) — NEED INVESTIGATION")
        narrative.append(f"*Cases closed in {week} that are P90+ outliers:*\n")
        
        # Show ALL new outliers
        for i, (_, row) in enumerate(new_outliers.iterrows(), 1):
            dev_id = row.get('dev_id', 'N/A')
            ttr = row['ttr_ops_overall']
            vertical = row.get('dev_reason', 'Unknown')
            classification = row['case_classification']
            site = row.get('site', '')
            
            flag = "🔴 External Hold" if classification == 'External Hold' else "🟡 Internal Ops"
            narrative.append(f"{i}. **{ttr:.1f} days** - {vertical} ({site}) - {flag} - ID: {dev_id}")
        
        narrative.append("")
    else:
        narrative.append("### 🆕 NEW Outliers This Week")
        narrative.append("✅ **No new P90+ outliers this week** — all outliers are recurring from prior weeks.\n")
    
    # === RECURRING OUTLIERS (Already Dived) ===
    if len(recurring_outliers) > 0:
        narrative.append(f"### 🔄 Recurring Outliers ({len(recurring_outliers)}) — Already Dived Previously")
        narrative.append(f"*Cases from prior weeks still in T4W window (no action needed):*\n")
        
        # Group by week
        for closed_week in sorted(recurring_outliers['year_week'].unique()):
            week_cases = recurring_outliers[recurring_outliers['year_week'] == closed_week]
            narrative.append(f"- **{closed_week}:** {len(week_cases)} cases (will roll off in future weeks)")
        
        narrative.append("")
    
    # === ROOT CAUSE BREAKDOWN (NEW ONLY) ===
    if len(new_outliers) > 0:
        narrative.append("### 🔍 Root Cause Breakdown (New Outliers Only)")
        for pattern in new_outlier_analysis['patterns']:
            narrative.append(f"- {pattern}")
        narrative.append("")
    
    # === OUTLOOK ===
    narrative.append("### 💡 Outlook")
    
    if len(recurring_outliers) > 0:
        # Calculate when recurring outliers will roll off
        for closed_week in sorted(recurring_outliers['year_week'].unique()):
            # Handle both formats: "WK16" (new) and "2026-W16" (old)
            if closed_week.startswith('WK'):
                week_num = int(closed_week.replace('WK', ''))
            else:
                week_num = int(closed_week.split('-W')[1])
            
            if week.startswith('WK'):
                current_week_num = int(week.replace('WK', ''))
            else:
                current_week_num = int(week.split('-W')[1])
            
            weeks_until_rolloff = 4 - (current_week_num - week_num)
            if weeks_until_rolloff > 0:
                count = len(recurring_outliers[recurring_outliers['year_week'] == closed_week])
                narrative.append(f"- {count} outliers from {closed_week} will roll off T4W in {weeks_until_rolloff} week(s)")
        narrative.append(f"- Monitor External Hold cases for policy team resolution")
    
    if current['external_pct'] > 15:
        narrative.append(f"- External Hold at {current['external_pct']}% — above typical levels")
    
    narrative.append("")
    
    # === ACTION ITEMS ===
    narrative.append("### ✅ Action Items")
    
    if len(new_outliers) > 0:
        if new_outlier_analysis['classification'].get('Internal Ops', 0) > 0:
            narrative.append(f"- [ ] Review {new_outlier_analysis['classification'].get('Internal Ops', 0)} NEW Internal Ops outliers for potential defects")
        if new_outlier_analysis['classification'].get('External Hold', 0) > 0:
            narrative.append(f"- [ ] Document {new_outlier_analysis['classification'].get('External Hold', 0)} NEW External Hold cases for WBR call-out")
        if new_outlier_analysis['top_verticals']:
            top_v = new_outlier_analysis['top_verticals'][0][0]
            narrative.append(f"- [ ] Deep dive {top_v} vertical for process improvement")
    else:
        narrative.append("- [x] No new outliers to investigate this week")
        narrative.append("- [ ] Monitor T4W trend as prior outliers roll off")
    
    return "\n".join(narrative)


def generate_wbr_table(df, selected_sites=None):
    """Generate a summary table for WBR."""
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    tp90_weekly = calculate_weekly_tp90(df, selected_sites)
    dart_weekly = calculate_weekly_dart_p90(df, selected_sites)
    
    if len(tp90_weekly) == 0:
        return pd.DataFrame()
    
    # Merge
    summary = tp90_weekly[['year_week', 'tp90', 'total_cases', 'external_hold_pct']].merge(
        dart_weekly[['year_week', 'dart_p90', 'case_count']],
        on='year_week',
        how='left'
    )
    
    # Calculate WoW changes
    summary['tp90_wow'] = summary['tp90'].diff().round(2)
    summary['dart_p90_wow'] = summary['dart_p90'].diff().round(2)
    
    # Format WoW with +/-
    summary['tp90_wow_str'] = summary['tp90_wow'].apply(
        lambda x: f"+{x}" if pd.notna(x) and x > 0 else str(x) if pd.notna(x) else ""
    )
    summary['dart_p90_wow_str'] = summary['dart_p90_wow'].apply(
        lambda x: f"+{x}" if pd.notna(x) and x > 0 else str(x) if pd.notna(x) else ""
    )
    
    return summary


def get_potential_defects(outliers_df):
    """
    Identify potential defects from outliers for logging to Issue Tracker.
    These are Internal Ops cases that may have controllable errors.
    Only flags NEW outliers (not recurring ones you already investigated).
    """
    if len(outliers_df) == 0:
        return pd.DataFrame()
    
    # Filter to Internal Ops only (External Hold is not controllable)
    internal_outliers = outliers_df[outliers_df['case_classification'] == 'Internal Ops'].copy()
    
    if len(internal_outliers) == 0:
        return pd.DataFrame()
    
    potential_defects = internal_outliers.copy()
    potential_defects['potential_defect_reason'] = 'High TTR - requires investigation'
    
    return potential_defects


def generate_wbr_callout_paragraph(df, week, selected_sites=None):
    """
    Generate an A+ WBR callout paragraph with real analysis and insights.
    
    Structure:
    1. Opening metrics line (TP90, Avg TTR, WoW changes)
    2. Primary driver with volume % and context
    3. T4W status vs Kingpin target
    4. Outlier analysis (controllable vs uncontrollable, opportunity sizing)
    5. Actions (specific, not generic)
    
    Returns: string paragraph ready for WBR
    """
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    # Get current week stats
    current_stats = get_weekly_stats(df, week, selected_sites)
    if not current_stats:
        return "No data available for the selected week."
    
    # Get WoW comparison
    wow = get_wow_comparison(df, week, selected_sites)
    if not wow:
        return "No comparison data available."
    
    current = wow['current']
    tp90_change = wow['tp90_change']
    avg_ttr_change = wow['avg_ttr_change']
    
    # Get T4W stats
    t4w_stats = get_t4w_stats(df, week, selected_sites)
    
    # Get outliers for this week
    new_outliers, recurring_outliers = get_outliers_with_new_flag(df, week, top_n=50, selected_sites=selected_sites)
    
    # Extract week number for display
    week_num = week.replace('WK', '')
    
    # Get all week cases for analysis
    all_cases = get_all_cases_by_sites(df, selected_sites)
    week_cases = all_cases[all_cases['year_week'] == week].drop_duplicates(subset=['dev_id'])
    total_cases = len(week_cases)
    
    # Determine if improvement or regression
    is_improvement = tp90_change < 0
    
    # Build the paragraph
    paragraph_parts = []
    
    # ===== PART 1: Opening metrics line =====
    tp90_wow_str = f"{tp90_change:+.2f}" if tp90_change != 0 else "flat"
    avg_wow_str = f"{avg_ttr_change:+.2f}" if avg_ttr_change != 0 else "flat"
    
    opening = f"Week {week_num} TP90 stood at {current['tp90']:.2f} days ({tp90_wow_str} WoW) with an average TTR of {current['avg_ttr']:.2f} days ({avg_wow_str} WoW)."
    paragraph_parts.append(opening)
    
    # ===== PART 2: Primary driver with volume context =====
    vertical_stats = week_cases.groupby('dev_reason').agg({
        'ttr_ops_overall': ['mean', 'count', lambda x: x.quantile(0.9)]
    }).round(2)
    vertical_stats.columns = ['avg_ttr', 'cases', 'p90']
    vertical_stats = vertical_stats.sort_values('cases', ascending=False)
    vertical_stats['pct'] = (vertical_stats['cases'] / total_cases * 100).round(0)
    
    top_vert = vertical_stats.index[0] if len(vertical_stats) > 0 else "Unknown"
    top_vert_cases = vertical_stats.iloc[0]['cases'] if len(vertical_stats) > 0 else 0
    top_vert_pct = vertical_stats.iloc[0]['pct'] if len(vertical_stats) > 0 else 0
    top_vert_avg = vertical_stats.iloc[0]['avg_ttr'] if len(vertical_stats) > 0 else 0
    
    if is_improvement:
        # Calculate improvement percentage
        improvement_pct = abs(tp90_change / (current['tp90'] + abs(tp90_change)) * 100)
        
        if improvement_pct >= 15:
            driver_text = f"The significant improvement was primarily driven by {top_vert}, which comprised {top_vert_pct:.0f}% of case volume ({int(top_vert_cases)} of {total_cases} cases) with an average resolve time of {top_vert_avg:.2f} days."
        else:
            driver_text = f"Performance remains stable, driven by {top_vert} ({top_vert_pct:.0f}% of volume) with an average resolve time of {top_vert_avg:.2f} days."
    else:
        # Regression - identify what's dragging
        # Find verticals with high P90
        high_p90_verts = vertical_stats[vertical_stats['p90'] > current['tp90']].head(3)
        
        if len(high_p90_verts) > 0:
            problem_verts = ", ".join(high_p90_verts.index.tolist())
            driver_text = f"The increase was primarily driven by elevated TTR in {problem_verts}."
        else:
            # Check external hold %
            external_pct = current.get('external_pct', 0)
            if external_pct > 15:
                driver_text = f"The increase was primarily driven by External Hold cases comprising {external_pct:.0f}% of volume, awaiting policy team reviews."
            else:
                second_vert = vertical_stats.index[1] if len(vertical_stats) > 1 else None
                second_avg = vertical_stats.iloc[1]['avg_ttr'] if len(vertical_stats) > 1 else 0
                driver_text = f"Performance driven by two primary deviation categories: {top_vert} averaging {top_vert_avg:.2f} days and {second_vert} at {second_avg:.2f} days."
    
    paragraph_parts.append(driver_text)
    
    # ===== PART 3: T4W status vs Kingpin target =====
    if t4w_stats:
        t4w_tp90 = t4w_stats.get('tp90', 0)  # Key is 'tp90' not 't4w_tp90'
        target = TP90_TARGET
        gap = t4w_tp90 - target
        
        if gap <= 0:
            t4w_text = f"T4W TP90 is currently at {t4w_tp90:.2f} days, {abs(gap):.2f} days under the Kingpin target of {target} days — ON TARGET."
        else:
            t4w_text = f"T4W TP90 is currently at {t4w_tp90:.2f} days, {gap:.2f} days above the Kingpin target of {target} days — requires attention."
        
        paragraph_parts.append(t4w_text)
    
    if len(new_outliers) > 0:
        # Categorize outliers
        internal_outliers = new_outliers[new_outliers['case_classification'] == 'Internal Ops']
        external_outliers = new_outliers[new_outliers['case_classification'] == 'External Hold']
        
        total_outliers = len(new_outliers)
        controllable = len(internal_outliers)
        uncontrollable = len(external_outliers)
        
        # Calculate impact of removing top outliers
        p90_threshold = week_cases['ttr_ops_overall'].quantile(0.9)
        outlier_cases = week_cases[week_cases['ttr_ops_overall'] >= p90_threshold]
        top3_ids = outlier_cases.nlargest(3, 'ttr_ops_overall')['dev_id'].tolist()
        without_top3 = week_cases[~week_cases['dev_id'].isin(top3_ids)]
        tp90_without_top3 = without_top3['ttr_ops_overall'].quantile(0.9) if len(without_top3) > 0 else current['tp90']
        potential_improvement = current['tp90'] - tp90_without_top3
        outlier_text = f"{total_outliers} P90+ outliers were identified, with {controllable} controllable (Internal Ops) and {uncontrollable} uncontrollable (External Hold)."
        paragraph_parts.append(outlier_text)
        
        # Max outlier details
        max_outlier = new_outliers.iloc[0]
        # Max outlier details
        max_ttr = max_outlier['ttr_ops_overall']
        max_vert = max_outlier.get('dev_reason', 'Unknown')
        max_class = max_outlier.get('case_classification', 'Unknown')
        
        if max_ttr > 20:
            if max_class == 'External Hold':
                max_text = f"The maximum outlier was a {max_vert} External Hold case at {max_ttr:.0f} days, awaiting external policy team review [Need investigation input]."
            else:
                max_text = f"The maximum outlier was a {max_vert} case at {max_ttr:.0f} days [Need investigation input]."
            paragraph_parts.append(max_text)
        
        # Controllable outlier breakdown (opportunity)
        if controllable > 0:
            # Group by vertical
            int_by_vert = internal_outliers.groupby('dev_reason').agg({
                'ttr_ops_overall': ['count', 'mean']
            }).round(2)
            int_by_vert.columns = ['count', 'avg_ttr']
            int_by_vert = int_by_vert.sort_values('count', ascending=False)
            if len(int_by_vert) > 0:
                top_problem_vert = int_by_vert.index[0]
                top_problem_count = int(int_by_vert.iloc[0]['count'])
                top_problem_avg = int_by_vert.iloc[0]['avg_ttr']
                
                if top_problem_count >= 2:
                    opp_text = f"Among controllable outliers, {top_problem_count} {top_problem_vert} cases averaged {top_problem_avg:.1f} days [Need investigation input]."
                    paragraph_parts.append(opp_text)
        
        # Opportunity sizing
        if potential_improvement > 0.3:
            sizing_text = f"Removing the top 3 outliers would reduce TP90 from {current['tp90']:.2f} to {tp90_without_top3:.2f} days ({potential_improvement:.2f} day improvement opportunity)."
            paragraph_parts.append(sizing_text)
        
        # External hold tracking
        if uncontrollable > 0:
            ext_verts = external_outliers['dev_reason'].unique()[:3]
            ext_list = ", ".join(ext_verts)
            ext_text = f"External policy team review remains the primary driver of uncontrollable outliers for {ext_list} [Need investigation input]."
            paragraph_parts.append(ext_text)
    
    else:
        paragraph_parts.append("No significant P90+ outliers were identified this week.")
    
    # ===== PART 5: Actions (specific, not generic) =====
    actions_needed = []
    
    if len(new_outliers) > 0:
        internal_outliers = new_outliers[new_outliers['case_classification'] == 'Internal Ops']
        external_outliers = new_outliers[new_outliers['case_classification'] == 'External Hold']
        
        if len(internal_outliers) > 0:
            # Group internal outliers by vertical
            int_by_vert = internal_outliers.groupby('dev_reason').size().sort_values(ascending=False)
            top_verts = int_by_vert.head(2).index.tolist()
            
            if len(top_verts) > 0:
                actions_needed.append(f"RCA required for {', '.join(top_verts)} outliers [Need investigation input]")
        
        if len(external_outliers) > 0:
            actions_needed.append("External Policy Team delays being actively tracked [Need investigation input]")
    
    if actions_needed:
        actions_text = "\n\nActions: " + "; ".join(actions_needed) + "."
        paragraph_parts.append(actions_text)
    
    return " ".join(paragraph_parts)
    return " ".join(paragraph_parts)


def get_wbr_callout_data(df, week, selected_sites=None):
    """
    Get all the data needed for WBR callout in a structured format.
    Useful for AI-assisted callout generation.
    """
    if selected_sites is None:
        selected_sites = APAC_SITES
    
    current_stats = get_weekly_stats(df, week, selected_sites)
    wow = get_wow_comparison(df, week, selected_sites)
    new_outliers, recurring_outliers = get_outliers_with_new_flag(df, week, top_n=20, selected_sites=selected_sites)
    t4w_stats = get_t4w_stats(df, week, selected_sites)
    
    # Vertical breakdown
    all_cases = get_all_cases_by_sites(df, selected_sites)
    week_cases = all_cases[all_cases['year_week'] == week].drop_duplicates(subset=['dev_id'])
    
    vertical_breakdown = week_cases.groupby('dev_reason').agg({
        'ttr_ops_overall': ['mean', 'count', lambda x: x.quantile(0.9)],
    }).round(2)
    vertical_breakdown.columns = ['avg_ttr', 'case_count', 'p90']
    vertical_breakdown = vertical_breakdown.sort_values('case_count', ascending=False)
    
    return {
        'week': week,
        'current_stats': current_stats,
        'wow_comparison': wow,
        't4w_stats': t4w_stats,
        'new_outliers': new_outliers.to_dict('records') if len(new_outliers) > 0 else [],
        'recurring_outliers': recurring_outliers.to_dict('records') if len(recurring_outliers) > 0 else [],
        'vertical_breakdown': vertical_breakdown.to_dict('index'),
        'total_new_outliers': len(new_outliers),
        'total_recurring_outliers': len(recurring_outliers),
        'internal_ops_outliers': len(new_outliers[new_outliers['case_classification'] == 'Internal Ops']) if len(new_outliers) > 0 else 0,
        'external_hold_outliers': len(new_outliers[new_outliers['case_classification'] == 'External Hold']) if len(new_outliers) > 0 else 0,
    }
