"""
TTR AI Workspace - Main Application
Two Dashboards: TP90 (Kingpin Goal T4W) + DART P90 (Internal Ops)
WBR Generator: Auto-generates weekly call-out narratives with NEW vs RECURRING outlier tracking
APAC Focus: SIN + HND | 2026 Data Only
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import os
import urllib.parse

from config import (
    PAGE_TITLE, PAGE_ICON, COLORS, 
    DART_P90_STRETCH, DART_P90_CEILING, 
    DATA_DIR, CLOSED_DEVIATION_FILE, APAC_SITES
)
from data_loader import (
    load_closed_deviation_data,
    calculate_weekly_dart_p90,
    calculate_manager_dart_p90,
    calculate_vertical_dart_p90,
    get_summary_stats,
    get_data_context_for_ai,
    get_internal_ops_by_sites,
    calculate_weekly_tp90,
    calculate_weekly_comparison,
    get_tp90_summary_stats,
    get_all_cases_by_sites,
    TP90_TARGET
)
from ai_chat import get_ai_response
from wbr_generator import (
    generate_wbr_narrative,
    generate_wbr_callout_paragraph,
    get_outliers,
    get_outliers_with_new_flag,
    get_wow_comparison,
    get_t4w_stats,
    analyze_outlier_root_causes,
    get_potential_defects,
    generate_wbr_table
)
from open_deviation_loader import (
    load_open_deviation_data,
    get_open_deviation_by_sites,
    get_open_deviation_summary,
    get_open_by_stage,
    get_open_by_vertical,
    get_alert_cases,
    get_ageing_distribution
)

# =============================================================================
# PAGE CONFIG
# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title=PAGE_TITLE,
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

selected_sites = APAC_SITES

# =============================================================================
# SESSION STATE FOR CASE EXPLORER FILTER
# =============================================================================

if 'explorer_filter_dev_id' not in st.session_state:
    st.session_state.explorer_filter_dev_id = None

# =============================================================================
# HELPER FUNCTION
# =============================================================================

def fmt(value):
    """Format number to 2 decimal places."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}"

# =============================================================================
# CUSTOM CSS
# =============================================================================

st.markdown("""
<style>
    .main-header { font-size: 2.5rem; font-weight: 700; color: #1E88E5; margin-bottom: 0; }
    .sub-header { font-size: 1rem; color: #666; margin-top: 0; }
    .chat-message { padding: 10px 15px; border-radius: 10px; margin: 5px 0; }
    .user-message { background-color: #E3F2FD; }
    .ai-message { background-color: #F5F5F5; }
    .wbr-output { background-color: #f8f9fa; padding: 20px; border-radius: 10px; border-left: 4px solid #1E88E5; }
    .new-outlier { background-color: #fff3e0; padding: 10px; border-radius: 5px; border-left: 3px solid #ff9800; margin: 5px 0; }
    .recurring-outlier { background-color: #e8f5e9; padding: 10px; border-radius: 5px; border-left: 3px solid #4caf50; margin: 5px 0; }
    .defect-flag { background-color: #ffebee; padding: 10px; border-radius: 5px; border-left: 3px solid #f44336; }
    .tp90-green { background-color: #C8E6C9; padding: 15px; border-radius: 10px; text-align: center; }
    .tp90-red { background-color: #FFCDD2; padding: 15px; border-radius: 10px; text-align: center; }
    .tp90-value { font-size: 2rem; font-weight: bold; }
    .tp90-label { font-size: 0.9rem; color: #666; }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# DATA LOADING
# =============================================================================

@st.cache_data
def load_data():
    data_path = Path(DATA_DIR) / CLOSED_DEVIATION_FILE
    if not data_path.exists():
        return None
    return load_closed_deviation_data(data_path)

def get_data_date_info(df):
    if df is None or len(df) == 0:
        return None
    min_date = df['dev_end_date_parsed'].min()
    max_date = df['dev_end_date_parsed'].max()
    return {
        'min_date': min_date.strftime('%d %b %Y'),
        'max_date': max_date.strftime('%d %b %Y'),
        'weeks': df['year_week'].nunique(),
        'latest_week': df['year_week'].max()
    }

def get_file_modified_time():
    data_path = Path(DATA_DIR) / CLOSED_DEVIATION_FILE
    if data_path.exists():
        mod_time = os.path.getmtime(data_path)
        return datetime.fromtimestamp(mod_time).strftime('%d %b %Y %H:%M')
    return None

# =============================================================================
# SIDEBAR
# =============================================================================

# Session state for refresh failure
if 'refresh_failed' not in st.session_state:
    st.session_state.refresh_failed = False

with st.sidebar:
    st.markdown("## TTR AI Workspace")
    st.caption("APAC (SIN + HND) | 2026")
    st.markdown("---")
    
    # Data Refresh Section
    st.markdown("### Data Refresh")
    
    # Auto refresh from DataCentral
    if st.button("🔄 Refresh from DataCentral", use_container_width=True, type="primary"):
        with st.spinner("Connecting to DataCentral..."):
            try:
                from data_refresh import refresh_data
                result = refresh_data()
                
                if result['success']:
                    st.success(result['message'])
                    st.session_state.refresh_failed = False
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(result['message'])
                    st.session_state.refresh_failed = True
            except Exception as e:
                st.error(f"Error: {e}")
                st.session_state.refresh_failed = True
    
    # Manual upload - only show if auto-refresh failed
    if st.session_state.refresh_failed:
        st.markdown("---")
        st.markdown("#### 📁 Manual Upload")
        st.caption("Auto-refresh failed. Upload CSV manually:")
        
        uploaded_file = st.file_uploader(
            "Upload CSV", 
            type=['csv', 'tsv'], 
            help="Download from DataCentral and upload here",
            label_visibility="collapsed"
        )
        
        if uploaded_file is not None:
            try:
                import pandas as pd
                
                # Detect separator
                content = uploaded_file.read().decode('utf-8')
                sep = '\t' if '\t' in content.split('\n')[0] else ','
                uploaded_file.seek(0)
                
                df_upload = pd.read_csv(uploaded_file, sep=sep)
                
                # Filter to APAC
                site_col = 'site_name' if 'site_name' in df_upload.columns else 'dev_end_site_name'
                if site_col in df_upload.columns:
                    df_upload = df_upload[df_upload[site_col].isin(['SIN', 'HND'])]
                
                # Save to data folder
                save_path = Path(DATA_DIR) / CLOSED_DEVIATION_FILE
                df_upload.to_csv(save_path, index=False)
                
                st.success(f"✅ Uploaded {len(df_upload):,} rows")
                st.session_state.refresh_failed = False
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Upload error: {e}")
    
    # Clear cache button
    if st.button("Clear Cache", use_container_width=True):
        st.cache_data.clear()

    st.markdown("---")
    
    st.markdown("### Data Status")
    data_path = Path(DATA_DIR) / CLOSED_DEVIATION_FILE
    if data_path.exists():
        df_check = load_data()
        if df_check is not None:
            st.success(f"2026 Data Loaded")
            st.caption(f"{len(df_check):,} records")
            date_info_sidebar = get_data_date_info(df_check)
            if date_info_sidebar:
                st.caption(f"{date_info_sidebar['min_date']} to {date_info_sidebar['max_date']}")
                st.caption(f"{date_info_sidebar['weeks']} weeks")
            file_mod = get_file_modified_time()
            if file_mod:
                st.caption(f"Updated: {file_mod}")
        else:
            st.warning("Data file exists but could not load")
    else:
        st.error("No data file")
    st.markdown("---")
    st.markdown("### Targets")
    st.markdown(f"**TP90 (Kingpin T4W)**")
    st.markdown(f"Target: {TP90_TARGET}d or less")
    st.markdown(f"**DART P90 (Internal Ops)**")
    st.markdown(f"Stretch: {DART_P90_STRETCH}d or less")
    st.markdown(f"Ceiling: {DART_P90_CEILING}d or less")
    
    st.markdown("---")
    
    # Page Navigation
    page = st.radio(
        "Navigation",
        ["TP90 Dashboard", "DART P90 Dashboard", "WBR Generator", "Open Deviation Monitor", "Manager Scorecard", "Vertical Analysis", "AI Chat", "Case Explorer", "Documentation"],
        label_visibility="collapsed"
    )

# Load data for main app
df = load_data()

if df is None:
    st.warning("⚠️ No data file found. Please upload data using the sidebar.")
    st.stop()

if df is not None:
    date_info = get_data_date_info(df)
    dart_stats = get_summary_stats(df, selected_sites)
    tp90_stats = get_tp90_summary_stats(df, selected_sites)
    dart_weekly = calculate_weekly_dart_p90(df, selected_sites)
    tp90_weekly = calculate_weekly_tp90(df, selected_sites)
    comparison = calculate_weekly_comparison(df, selected_sites)
    by_manager = calculate_manager_dart_p90(df, selected_sites)
    by_vertical = calculate_vertical_dart_p90(df, selected_sites)
    data_context = get_data_context_for_ai(df, selected_sites)
    t4w_weeks = tp90_stats.get('t4w_weeks', [])
    t4w_weeks_str = f"{t4w_weeks[0]} → {t4w_weeks[-1]}" if len(t4w_weeks) >= 2 else ', '.join(t4w_weeks)
else:
    date_info = None
    t4w_weeks_str = "N/A"

# =============================================================================
# PAGE: WBR GENERATOR
# =============================================================================

if page == "WBR Generator":
    st.markdown('<p class="main-header">📝 WBR Generator</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Auto-generate Weekly Business Review Call-Outs | NEW vs RECURRING Outlier Tracking</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    all_cases = get_all_cases_by_sites(df, selected_sites)
    weeks = sorted(all_cases['year_week'].unique(), reverse=True)
    
    # Week selector at top
    selected_week = st.selectbox("Select Week", weeks, index=0)
    
    # Get T4W stats for the selected week
    t4w_stats = get_t4w_stats(df, selected_week, selected_sites)
    wow = get_wow_comparison(df, selected_week, selected_sites)
    
    st.markdown("---")
    
    # =========================================================================
    # TP90 INDICATOR - PROMINENT DISPLAY
    # =========================================================================
    
    if t4w_stats:
        t4w_tp90 = t4w_stats['tp90']
        is_on_target = t4w_tp90 <= TP90_TARGET
        status_class = "tp90-green" if is_on_target else "tp90-red"
        status_icon = "✅" if is_on_target else "⚠️"
        gap = round(t4w_tp90 - TP90_TARGET, 2)
        gap_text = f"{abs(gap):.2f}d {'below' if gap < 0 else 'above'} target"
        
    if t4w_stats:
        t4w_tp90 = t4w_stats['tp90']
        is_on_target = t4w_tp90 <= TP90_TARGET
        status_class = "tp90-green" if is_on_target else "tp90-red"
        status_icon = "YES" if is_on_target else "NO"
        gap = round(t4w_tp90 - TP90_TARGET, 2)
        gap_text = f"{abs(gap):.2f}d {'below' if gap < 0 else 'above'} target"
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="{status_class}">
                <div class="tp90-label">T4W TP90 ({status_icon})</div>
                <div class="tp90-value">{t4w_tp90:.2f}d</div>
                <div class="tp90-label">Target: {TP90_TARGET}d | {gap_text}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style="background-color: #E3F2FD; padding: 15px; border-radius: 10px; text-align: center;">
                <div class="tp90-label">T4W Period</div>
                <div style="font-size: 1.2rem; font-weight: bold;">{t4w_stats['week_range']}</div>
                <div class="tp90-label">{t4w_stats['total_cases']:,} cases</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div style="background-color: #F3E5F5; padding: 15px; border-radius: 10px; text-align: center;">
                <div class="tp90-label">External Hold Pct</div>
                <div style="font-size: 1.5rem; font-weight: bold;">{t4w_stats['external_pct']:.1f}%</div>
                <div class="tp90-label">of T4W cases</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.info("NEW vs RECURRING Outliers: NEW = Cases closed THIS week that are P90+ outliers (Need investigation). RECURRING = Cases from prior weeks still in T4W window (Already dived, no action needed). This prevents re-investigating the same cases every week!")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("### Quick Actions")
        callout_btn = st.button("📝 Generate WBR Callout", use_container_width=True, type="primary")
        generate_btn = st.button("📊 Generate Full Analysis", use_container_width=True, type="secondary")
        
        st.markdown("---")
        
        if wow:
            current = wow['current']
            st.markdown("### Week Stats")
            st.metric("Weekly TP90", f"{current['tp90']} days", f"{wow['tp90_change']:+.2f} WoW")
            st.metric("Avg TTR", f"{current['avg_ttr']} days", f"{wow['avg_ttr_change']:+.2f} WoW")
            st.metric("Cases", current['total_cases'], f"{wow['case_count_change']:+d} WoW")
            st.metric("External %", f"{current['external_pct']}%", f"{wow['external_pct_change']:+.1f}% WoW")
    
    with col2:
        # Generate WBR Callout Paragraph (Smart Option A)
        if callout_btn:
            with st.spinner("Generating WBR callout..."):
                callout = generate_wbr_callout_paragraph(df, selected_week, selected_sites)
                st.session_state.wbr_callout = callout
                st.session_state.wbr_callout_week = selected_week
        
        if 'wbr_callout' in st.session_state and st.session_state.get('wbr_callout_week') == selected_week:
            st.markdown("### 📝 Ready-to-Paste WBR Callout")
            st.info("Copy this paragraph directly into your WBR:")
            
            st.text_area(
                "WBR Callout",
                st.session_state.wbr_callout,
                height=250,
                key="callout_text",
                label_visibility="collapsed"
            )
            
            st.download_button(
                "📥 Download Callout",
                st.session_state.wbr_callout,
                file_name=f"WBR_Callout_{selected_week}.txt",
                mime="text/plain",
                use_container_width=True
            )
        
        # Full Analysis (existing functionality)
        if generate_btn:
            with st.spinner("Generating full WBR analysis..."):
                narrative = generate_wbr_narrative(df, selected_week, selected_sites)
                st.session_state.wbr_narrative = narrative
                st.session_state.wbr_week = selected_week
        
        if 'wbr_narrative' in st.session_state and st.session_state.get('wbr_week') == selected_week and not callout_btn:
            st.markdown("### 📊 Full WBR Analysis")
            st.markdown('<div class="wbr-output">', unsafe_allow_html=True)
            st.markdown(st.session_state.wbr_narrative)
            st.markdown('</div>', unsafe_allow_html=True)
            
            st.download_button(
                "📥 Download Full Analysis",
                st.session_state.wbr_narrative,
                file_name=f"WBR_Closed_Deviation_{selected_week}.txt",
                mime="text/plain"
            )
    
    st.markdown("---")
    
    # NEW vs RECURRING Outliers Section
    st.markdown("### P90+ Outliers - NEW vs RECURRING")
    
    new_outliers, recurring_outliers = get_outliers_with_new_flag(df, selected_week, top_n=50, selected_sites=selected_sites)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("NEW (Need Investigation)", len(new_outliers))
    with col2:
        st.metric("RECURRING (Already Dived)", len(recurring_outliers))
    with col3:
        total_outliers = len(new_outliers) + len(recurring_outliers)
        st.metric("Total T4W Outliers", total_outliers)
    with col4:
        new_internal = len(new_outliers[new_outliers['case_classification'] == 'Internal Ops']) if len(new_outliers) > 0 else 0
        st.metric("Internal Ops (Potential Defects)", new_internal)
    
    # Tabs for NEW vs RECURRING
    tab1, tab2 = st.tabs(["NEW Outliers (Need Investigation)", "RECURRING Outliers (Already Dived)"])
    
    with tab1:
        if len(new_outliers) > 0:
            st.markdown(f"**{len(new_outliers)} cases closed in {selected_week} that are P90+ outliers:**")
    tab1, tab2 = st.tabs(["🆕 NEW Outliers (Need Investigation)", "🔄 RECURRING Outliers (Already Dived)"])
    
    with tab1:
        if len(new_outliers) > 0:
            st.markdown(f"**{len(new_outliers)} cases closed in {selected_week} that are P90+ outliers:**")
            st.caption("💡 Click 'View' to see full case details in Case Explorer")
            
            # Create table with View buttons
            for idx, (_, row) in enumerate(new_outliers.iterrows()):
                dev_id = row.get('dev_id', 'N/A')
                ttr = row['ttr_overall']
                vertical = row.get('dev_reason', 'Unknown')
                classification = row['case_classification']
                site = row.get('site', '')
                
                flag_color = "#ffebee" if classification == 'Internal Ops' else "#fff3e0"
                flag_text = "🟡 Internal Ops" if classification == 'Internal Ops' else "🔴 External Hold"
                
                col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 1, 2, 2, 1])
                
                with col1:
                    st.markdown(f"**{ttr:.2f} days**")
                with col2:
                    st.markdown(f"{vertical}")
                with col3:
                    st.markdown(f"{site}")
                with col4:
                    st.markdown(f"{flag_text}")
                with col5:
                    st.markdown(f"`{dev_id[:8]}...`")
                with col6:
                    if st.button("🔍 View", key=f"view_new_{idx}", help=f"View case {dev_id} in Case Explorer"):
                        st.session_state.explorer_filter_dev_id = dev_id
                        st.switch_page = "📋 Case Explorer"
                        st.rerun()
                
                st.markdown("---")
            
            # Potential defects section
            new_internal_ops = new_outliers[new_outliers['case_classification'] == 'Internal Ops']
            if len(new_internal_ops) > 0:
                st.markdown("### 🔍 Potential Defects (NEW Internal Ops Outliers)")
                st.caption("These cases should be reviewed for Issue Tracker logging:")
                
                for idx, (_, row) in enumerate(new_internal_ops.iterrows()):
                    dev_id = row.get('dev_id', 'N/A')
                    ttr_val = f"{row['ttr_overall']:.2f}"
                    vertical = row.get('dev_reason', 'N/A')
                    site = row.get('site', 'N/A')
                    
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f'<div class="defect-flag">', unsafe_allow_html=True)
                        st.markdown(f"**Dev ID:** {dev_id} | **TTR:** {ttr_val} days | **Vertical:** {vertical} | **Site:** {site}")
                        st.markdown("🔎 *Internal Ops outlier - investigate for controllable defect*")
                        st.markdown('</div>', unsafe_allow_html=True)
                    with col2:
                        if st.button("🔍 View", key=f"view_defect_{idx}", help=f"View case {dev_id}"):
                            st.session_state.explorer_filter_dev_id = dev_id
                            st.rerun()
        else:
            st.success("✅ **No NEW outliers this week!** All P90+ cases are recurring from prior weeks.")
            st.markdown("This means no new investigation is needed — previous weeks' outliers are just rolling through T4W.")
    
    with tab2:
        if len(recurring_outliers) > 0:
            st.markdown(f"**{len(recurring_outliers)} cases from prior weeks still in T4W window:**")
            st.caption("These were already investigated in previous weeks — no action needed.")
            
            # Group by closed week
            for closed_week in sorted(recurring_outliers['year_week'].unique(), key=lambda x: int(x.replace('WK', ''))):
                week_cases = recurring_outliers[recurring_outliers['year_week'] == closed_week]
                
                # Calculate when they roll off (week format is WK16, not 2026-W16)
                closed_week_num = int(closed_week.replace('WK', ''))
                current_week_num = int(selected_week.replace('WK', ''))
                weeks_in_t4w = current_week_num - closed_week_num + 1
                weeks_until_rolloff = 4 - weeks_in_t4w + 1
                
                st.markdown(f'<div class="recurring-outlier">', unsafe_allow_html=True)
                st.markdown(f"**{closed_week}** — {len(week_cases)} cases | Rolls off T4W in **{weeks_until_rolloff} week(s)**")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Show table with View buttons
            st.markdown("---")
            for idx, (_, row) in enumerate(recurring_outliers.iterrows()):
                dev_id = row.get('dev_id', 'N/A')
                ttr = row['ttr_overall']
                vertical = row.get('dev_reason', 'Unknown')
                closed_week = row['year_week']
                
                col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 1])
                
                with col1:
                    st.markdown(f"**{ttr:.2f} days**")
                with col2:
                    st.markdown(f"{vertical}")
                with col3:
                    st.markdown(f"Closed: {closed_week}")
                with col4:
                    st.markdown(f"`{dev_id[:8]}...`")
                with col5:
                    if st.button("🔍 View", key=f"view_rec_{idx}", help=f"View case {dev_id}"):
                        st.session_state.explorer_filter_dev_id = dev_id
                        st.rerun()
        else:
            st.info("No recurring outliers from prior weeks.")
    
    st.markdown("---")
    
    # Weekly Summary Table
    st.markdown("### 📊 Weekly Summary Table (for WBR)")
    
    wbr_table = generate_wbr_table(df, selected_sites)
    if len(wbr_table) > 0:
        display_table = wbr_table[['year_week', 'tp90', 'tp90_wow_str', 'dart_p90', 'dart_p90_wow_str', 'total_cases', 'external_hold_pct']].copy()
        display_table.columns = ['Week', 'TP90', 'TP90 WoW', 'DART P90', 'DART WoW', 'Cases', 'Ext %']
        display_table['TP90'] = display_table['TP90'].apply(lambda x: f"{x:.2f}")
        display_table['DART P90'] = display_table['DART P90'].apply(lambda x: f"{x:.2f}")
        display_table['Ext %'] = display_table['Ext %'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(display_table.tail(8), hide_index=True, use_container_width=True)
        
        csv = wbr_table.to_csv(index=False)
        st.download_button("📥 Download Full Table", csv, file_name="WBR_Weekly_Summary.csv", mime="text/csv")

# =============================================================================
# PAGE: TP90 DASHBOARD
# =============================================================================

elif page == "TP90 Dashboard":
    st.markdown('<p class="main-header">📈 TP90 Dashboard</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">Kingpin Goal (T4W) | All Cases | APAC | 2026</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        status_icon = "🟢" if tp90_stats['t4w_status'] == 'Green' else "🔴"
        st.metric("T4W TP90", f"{fmt(tp90_stats['t4w_tp90'])} days", f"{status_icon} Target: {TP90_TARGET}d")
    with col2:
        st.metric("T4W Period", t4w_weeks_str, f"{tp90_stats['t4w_case_count']:,} cases")
    with col3:
        st.metric("T4W External Hold %", f"{tp90_stats['t4w_external_pct']:.1f}%", f"{tp90_stats['t4w_external_count']:,} cases")
    with col4:
        hit_rate = round(tp90_stats['green_weeks'] / tp90_stats['total_weeks'] * 100, 1) if tp90_stats['total_weeks'] > 0 else 0
        st.metric("Weekly On Target", f"{hit_rate}%", f"{tp90_stats['green_weeks']}/{tp90_stats['total_weeks']} weeks")
    
    st.markdown("---")
    st.subheader("📊 TP90 vs DART P90 Weekly Trend")
    
    if len(comparison) > 0:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=comparison['year_week'], y=comparison['tp90'], mode='lines+markers',
                                  name='TP90 (All Cases)', line=dict(color='#FF6B6B', width=3),
                                  hovertemplate='%{x}<br>TP90: %{y:.2f} days<extra></extra>'))
        fig.add_trace(go.Scatter(x=comparison['year_week'], y=comparison['dart_p90'], mode='lines+markers',
                                  name='DART P90 (Internal Ops)', line=dict(color=COLORS['primary'], width=3),
                                  hovertemplate='%{x}<br>DART P90: %{y:.2f} days<extra></extra>'))
        fig.add_hline(y=TP90_TARGET, line_dash="dash", line_color=COLORS['red'], annotation_text=f"TP90 Target ({TP90_TARGET}d)")
        fig.add_hline(y=DART_P90_CEILING, line_dash="dash", line_color=COLORS['green'], annotation_text=f"DART Ceiling ({DART_P90_CEILING}d)")
        fig.update_layout(xaxis_title="Week", yaxis_title="P90 (Days)", hovermode="x unified", height=400, legend=dict(orientation="h", yanchor="bottom", y=1.02))
        fig.update_yaxes(tickformat=".2f")
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Weekly TP90 Detail")
        if len(tp90_weekly) > 0:
            tp90_display = tp90_weekly[['year_week', 'tp90', 'total_cases', 'internal_ops_count', 'external_hold_count', 'external_hold_pct', 'status']].copy()
            tp90_display['tp90'] = tp90_display['tp90'].apply(lambda x: f"{x:.2f}")
            tp90_display['external_hold_pct'] = tp90_display['external_hold_pct'].apply(lambda x: f"{x:.1f}")
            tp90_display.columns = ['Week', 'TP90', 'Total', 'Internal', 'External', 'Ext %', 'Status']
            def highlight_status(row):
                color = '#C8E6C9' if row['Status'] == 'Green' else '#FFCDD2'
                return [f'background-color: {color}'] * len(row)
            st.dataframe(tp90_display.style.apply(highlight_status, axis=1), hide_index=True, use_container_width=True)
    
    with col2:
        st.subheader("📊 T4W Case Mix")
        if tp90_stats['t4w_case_count'] > 0:
            mix_data = pd.DataFrame({'Type': ['Internal Ops', 'External Hold'], 'Cases': [tp90_stats['t4w_internal_count'], tp90_stats['t4w_external_count']]})
            fig_mix = px.pie(mix_data, values='Cases', names='Type', color='Type',
                            color_discrete_map={'Internal Ops': COLORS['primary'], 'External Hold': '#FF6B6B'}, hole=0.4)
            fig_mix.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_mix, use_container_width=True)
    
    st.markdown("---")
    st.subheader("📈 External Hold Impact (TP90 - DART P90 Gap)")
    if len(comparison) > 0:
        fig_gap = px.bar(comparison, x='year_week', y='gap', labels={'gap': 'Gap (days)', 'year_week': 'Week'},
                         color='gap', color_continuous_scale=['green', 'yellow', 'red'])
        fig_gap.update_layout(height=300, showlegend=False)
        fig_gap.update_yaxes(tickformat=".2f")
        st.plotly_chart(fig_gap, use_container_width=True)
        avg_gap = comparison['gap'].mean()
        st.info(f"💡 **Average Gap:** {avg_gap:.2f} days — External Hold cases add this much to overall TP90.")

# =============================================================================
# PAGE: DART P90 DASHBOARD
# =============================================================================

elif page == "DART P90 Dashboard":
    st.markdown('<p class="main-header">📊 DART P90 Dashboard</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">Internal Ops Only | APAC | 2026</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("T4W DART P90", f"{fmt(dart_stats['t4w_dart_p90'])} days", f"{dart_stats['t4w_status']}")
    with col2:
        t4w_dart_weeks = dart_stats.get('t4w_weeks', [])
        t4w_dart_str = f"{t4w_dart_weeks[0]} → {t4w_dart_weeks[-1]}" if len(t4w_dart_weeks) >= 2 else ''
        st.metric("T4W Period", t4w_dart_str, f"{dart_stats['t4w_case_count']:,} cases")
    with col3:
        st.metric("YTD P90", f"{fmt(dart_stats['overall_p90'])} days", f"{dart_stats['total_cases']:,} cases")
    with col4:
        hit_rate = round(dart_stats['green_weeks'] / dart_stats['total_weeks'] * 100, 1) if dart_stats['total_weeks'] > 0 else 0
        st.metric("Green Week Rate", f"{hit_rate}%", f"{dart_stats['green_weeks']}/{dart_stats['total_weeks']} weeks")
    
    st.markdown("---")
    st.subheader("📈 Weekly DART P90 Trend")
    
    if len(dart_weekly) > 0:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dart_weekly['year_week'], y=dart_weekly['dart_p90'], mode='lines+markers',
                                  name='DART P90', line=dict(color=COLORS['primary'], width=3), marker=dict(size=10),
                                  hovertemplate='%{x}<br>DART P90: %{y:.2f} days<extra></extra>'))
        fig.add_hline(y=DART_P90_STRETCH, line_dash="dash", line_color=COLORS['green'], annotation_text=f"Stretch ({DART_P90_STRETCH}d)")
        fig.add_hline(y=DART_P90_CEILING, line_dash="dash", line_color=COLORS['red'], annotation_text=f"Ceiling ({DART_P90_CEILING}d)")
        fig.update_layout(xaxis_title="Week", yaxis_title="DART P90 (Days)", hovermode="x unified", height=400)
        fig.update_yaxes(tickformat=".2f")
        st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Weekly Performance")
        if len(dart_weekly) > 0:
            weekly_display = dart_weekly[['year_week', 'dart_p90', 'case_count', 'status']].copy()
            weekly_display['dart_p90'] = weekly_display['dart_p90'].apply(lambda x: f"{x:.2f}")
            weekly_display.columns = ['Week', 'DART P90', 'Cases', 'Status']
            def highlight_status(row):
                colors = {'Green': '#C8E6C9', 'Yellow': '#FFF9C4', 'Red': '#FFCDD2'}
                return [f'background-color: {colors.get(row["Status"], "")}'] * len(row)
            st.dataframe(weekly_display.style.apply(highlight_status, axis=1), hide_index=True, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Status Distribution")
        if dart_stats['total_weeks'] > 0:
            status_counts = pd.DataFrame({'Status': ['Green', 'Yellow', 'Red'], 'Weeks': [dart_stats['green_weeks'], dart_stats['yellow_weeks'], dart_stats['red_weeks']]})
            fig_pie = px.pie(status_counts, values='Weeks', names='Status', color='Status',
                            color_discrete_map={'Green': COLORS['green'], 'Yellow': COLORS['yellow'], 'Red': COLORS['red']}, hole=0.4)
            fig_pie.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_pie, use_container_width=True)
            st.plotly_chart(fig_pie, use_container_width=True)

# =============================================================================
# PAGE: OPEN DEVIATION MONITOR
# =============================================================================

elif page == "Open Deviation Monitor":
    st.markdown('<p class="main-header">🚨 Open Deviation Monitor</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Real-time Aging Alerts | 12+ Days = Action Required | APAC</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Load open deviation data
    open_df = load_open_deviation_data()
    
    if open_df is None or len(open_df) == 0:
        st.warning("⚠️ No open deviation data found. Please upload '2026 Open Deviation WW.csv' to the data folder.")
        st.stop()
    
    # Get APAC data
    open_summary = get_open_deviation_summary(open_df, selected_sites)
    
    # Alert threshold
    ALERT_THRESHOLD = 12
    
    # Summary Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Open", open_summary['total'])
    
    with col2:
        alert_color = "🟢" if open_summary['alert_count'] == 0 else "🔴"
        st.metric(f"{alert_color} Alerts (≥12d)", open_summary['alert_count'])
    
    with col3:
        st.metric("🟡 Watch (8-11d)", open_summary['watch'])
    
    with col4:
        st.metric("Avg Ageing", f"{open_summary['avg_ageing']}d")
    
    with col5:
        st.metric("Max Ageing", f"{open_summary['max_ageing']}d")
    
    st.markdown("---")
    
    # Alert status banner
    if open_summary['alert_count'] > 0:
        severe_count = open_summary['severe']
        critical_count = open_summary['critical']
        warning_count = open_summary['warning']
        
        if severe_count > 0:
            st.error(f"🚨 **SEVERE ALERT**: {severe_count} case(s) aged 21+ days require immediate escalation!")
        if critical_count > 0:
            st.warning(f"🔴 **CRITICAL**: {critical_count} case(s) aged 15-20 days need POC attention")
        if warning_count > 0:
            st.info(f"🟠 **WARNING**: {warning_count} case(s) aged 12-14 days — manager review required")
    else:
        st.success("✅ **All Clear**: No cases exceeding 12-day threshold")
    
    st.markdown("---")
    
    # Two columns: Charts and Alert Cases
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📊 Ageing Distribution")
        
        # Get distribution data
        dist_df = get_ageing_distribution(open_df, selected_sites)
        
        if len(dist_df) > 0:
            # Color coding for bars
            colors = ['#C8E6C9', '#C8E6C9', '#FFF9C4', '#FFE0B2', '#FFCDD2', '#F44336', '#B71C1C']
            
            fig = px.bar(dist_df, x='Bucket', y='Count', text='Count',
                        color='Bucket',
                        color_discrete_sequence=colors)
            fig.update_traces(textposition='outside')
            fig.update_layout(height=350, showlegend=False, xaxis_title="Days Open", yaxis_title="Cases")
            st.plotly_chart(fig, use_container_width=True)
        
        # By Stage breakdown
        st.subheader("📈 By Ops Stage")
        stage_df = get_open_by_stage(open_df, selected_sites)
        
        if len(stage_df) > 0:
            fig_stage = px.pie(stage_df, values='count', names='ttr_stage', hole=0.4,
                              color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'])
            fig_stage.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_stage, use_container_width=True)
    
    with col2:
        st.subheader(f"⚠️ Alert Cases (≥{ALERT_THRESHOLD} days)")
        
        alert_cases = get_alert_cases(open_df, selected_sites, min_days=ALERT_THRESHOLD)
        
        if len(alert_cases) > 0:
            st.caption(f"**{len(alert_cases)} cases** require attention")
            
            for idx, row in alert_cases.iterrows():
                days = int(row['ageing_days'])
                dev_id = row.get('dev_id', 'N/A')
                vertical = row.get('dev_reason', 'Unknown')
                stage = row.get('ttr_stage', 'Unknown')
                site = row.get('site_name', '')
                
                # Alert level styling
                if days >= 21:
                    alert_style = "background-color: #FFCDD2; border-left: 4px solid #F44336;"
                    alert_icon = "🚨"
                elif days >= 15:
                    alert_style = "background-color: #FFE0B2; border-left: 4px solid #FF9800;"
                    alert_icon = "🔴"
                else:
                    alert_style = "background-color: #FFF9C4; border-left: 4px solid #FFC107;"
                    alert_icon = "🟠"
                
                st.markdown(f'''
                <div style="{alert_style} padding: 10px; margin-bottom: 8px; border-radius: 4px;">
                    <strong>{alert_icon} {days} days</strong> | {vertical} | {stage}<br>
                    <small style="color: #666;">{dev_id[:20]}... | {site}</small>
                </div>
                ''', unsafe_allow_html=True)
        else:
            st.success("✅ No cases exceeding alert threshold!")
    
    st.markdown("---")
    
    # Detailed Tables
    st.subheader("📋 Detailed Breakdown")
    
    tab1, tab2, tab3 = st.tabs(["By Vertical", "By Stage", "All Open Cases"])
    
    with tab1:
        vert_df = get_open_by_vertical(open_df, selected_sites)
        if len(vert_df) > 0:
            vert_df['avg_days'] = vert_df['avg_days'].apply(lambda x: f"{x:.1f}")
            vert_df.columns = ['Vertical', 'Count', 'Avg Days', 'Max Days', 'Unique']
            st.dataframe(vert_df, hide_index=True, use_container_width=True)
    
    with tab2:
        stage_detail = get_open_by_stage(open_df, selected_sites)
        if len(stage_detail) > 0:
            stage_detail['avg_days'] = stage_detail['avg_days'].apply(lambda x: f"{x:.1f}")
            stage_detail.columns = ['Stage', 'Count', 'Avg Days', 'Max Days', 'Unique']
            st.dataframe(stage_detail, hide_index=True, use_container_width=True)
    
    with tab3:
        open_apac = get_open_deviation_by_sites(open_df, selected_sites)
        if len(open_apac) > 0:
            display_cols = ['dev_id', 'dev_reason', 'ageing_days', 'ttr_stage', 'site_name', 'supervisor_id', 'alert_level']
            available_cols = [c for c in display_cols if c in open_apac.columns]
            display_df = open_apac[available_cols].sort_values('ageing_days', ascending=False)
            
            def highlight_alert(row):
                if row.get('alert_level') == 'Severe':
                    return ['background-color: #FFCDD2'] * len(row)
                elif row.get('alert_level') == 'Critical':
                    return ['background-color: #FFE0B2'] * len(row)
                elif row.get('alert_level') == 'Warning':
                    return ['background-color: #FFF9C4'] * len(row)
                return [''] * len(row)
            
            st.dataframe(display_df.style.apply(highlight_alert, axis=1), hide_index=True, use_container_width=True)
            
            # Download button
            csv = open_apac.to_csv(index=False)
            st.download_button("📥 Download Open Deviations", csv, file_name="open_deviations_apac.csv", mime="text/csv")

# =============================================================================
# PAGE: MANAGER SCORECARD
# =============================================================================

elif page == "Manager Scorecard":
    st.markdown("## 👥 Manager Scorecard")
    st.caption(f"DART P90 by supervisor | APAC | 2026")
    st.markdown("---")
    
    if len(by_manager) == 0:
        st.warning("No manager data available.")
        st.stop()
    
    weeks = sorted(by_manager['year_week'].unique(), reverse=True)
    selected_week = st.selectbox("Select Week", weeks)
    week_data = by_manager[by_manager['year_week'] == selected_week].sort_values('dart_p90', ascending=False)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Managers", len(week_data))
    with col2:
        st.metric("🟢 Green", (week_data['status'] == 'Green').sum())
    with col3:
        st.metric("🟡 Yellow", (week_data['status'] == 'Yellow').sum())
    with col4:
        st.metric("🔴 Red", (week_data['status'] == 'Red').sum())
    
    st.markdown("---")
    st.subheader(f"DART P90 by Manager - {selected_week}")
    
    fig = px.bar(week_data, x='dev_end_supervisor_id', y='dart_p90', color='status',
                 color_discrete_map={'Green': COLORS['green'], 'Yellow': COLORS['yellow'], 'Red': COLORS['red']},
                 text=week_data['dart_p90'].apply(lambda x: f"{x:.2f}"), hover_data=['case_count'])
    fig.add_hline(y=DART_P90_STRETCH, line_dash="dash", line_color=COLORS['green'])
    fig.add_hline(y=DART_P90_CEILING, line_dash="dash", line_color=COLORS['red'])
    fig.update_layout(xaxis_title="Manager", yaxis_title="DART P90 (Days)", height=500)
    fig.update_traces(textposition='outside')
    fig.update_yaxes(tickformat=".2f")
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("📋 Manager Details")
    manager_display = week_data[['dev_end_supervisor_id', 'dart_p90', 'dart_median', 'case_count', 'status']].copy()
    manager_display['dart_p90'] = manager_display['dart_p90'].apply(lambda x: f"{x:.2f}")
    manager_display['dart_median'] = manager_display['dart_median'].apply(lambda x: f"{x:.2f}")
    manager_display.columns = ['Manager', 'DART P90', 'Median', 'Cases', 'Status']
    def highlight_status(row):
        colors = {'Green': '#C8E6C9', 'Yellow': '#FFF9C4', 'Red': '#FFCDD2'}
        return [f'background-color: {colors.get(row["Status"], "")}'] * len(row)
    st.dataframe(manager_display.style.apply(highlight_status, axis=1), hide_index=True, use_container_width=True)

# =============================================================================
# PAGE: VERTICAL ANALYSIS
# =============================================================================

elif page == "Vertical Analysis":
    st.markdown("## 📁 Vertical Analysis")
    st.caption(f"DART P90 by dev_reason | APAC | 2026")
    st.markdown("---")
    
    if len(by_vertical) == 0:
        st.warning("No vertical data available.")
        st.stop()
    
    weeks = sorted(by_vertical['year_week'].unique(), reverse=True)
    selected_week = st.selectbox("Select Week", weeks)
    week_data = by_vertical[by_vertical['year_week'] == selected_week].sort_values('dart_p90', ascending=True)
    
    st.subheader(f"DART P90 by Vertical - {selected_week}")
    
    fig = px.bar(week_data, y='dev_reason', x='dart_p90', color='status', orientation='h',
                 color_discrete_map={'Green': COLORS['green'], 'Yellow': COLORS['yellow'], 'Red': COLORS['red']},
                 text=week_data['dart_p90'].apply(lambda x: f"{x:.2f}"), hover_data=['case_count'])
    fig.add_vline(x=DART_P90_STRETCH, line_dash="dash", line_color=COLORS['green'])
    fig.add_vline(x=DART_P90_CEILING, line_dash="dash", line_color=COLORS['red'])
    fig.update_layout(xaxis_title="DART P90 (Days)", yaxis_title="Vertical", height=600)
    fig.update_traces(textposition='outside')
    fig.update_xaxes(tickformat=".2f")
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("📋 Vertical Details")
    vertical_display = week_data[['dev_reason', 'dart_p90', 'dart_median', 'case_count', 'status']].copy()
    vertical_display['dart_p90'] = vertical_display['dart_p90'].apply(lambda x: f"{x:.2f}")
    vertical_display['dart_median'] = vertical_display['dart_median'].apply(lambda x: f"{x:.2f}")
    vertical_display.columns = ['Vertical', 'DART P90', 'Median', 'Cases', 'Status']
    def highlight_status(row):
        colors = {'Green': '#C8E6C9', 'Yellow': '#FFF9C4', 'Red': '#FFCDD2'}
        return [f'background-color: {colors.get(row["Status"], "")}'] * len(row)
    st.dataframe(vertical_display.style.apply(highlight_status, axis=1), hide_index=True, use_container_width=True)

# =============================================================================
# PAGE: AI CHAT
# =============================================================================

elif page == "AI Chat":
    st.markdown("## 🤖 AI Chat")
    st.caption(f"Ask questions about TP90 & DART P90 | 2026 Data")
    st.markdown("---")
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    st.markdown("**Quick Questions:**")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📊 T4W Status"):
            st.session_state.quick_question = "What is the current T4W TP90 and DART P90 status?"
    with col2:
        if st.button("📈 TP90 vs DART"):
            st.session_state.quick_question = "Compare T4W TP90 and DART P90. What's the gap?"
    with col3:
        if st.button("⚠️ External Hold Impact"):
            st.session_state.quick_question = "How much are External Hold cases impacting T4W TP90?"
    
    col4, col5, col6 = st.columns(3)
    with col4:
        if st.button("💡 Recommendations"):
            st.session_state.quick_question = "What are your top 3 recommendations to hit Kingpin?"
    with col5:
        if st.button("📝 Weekly Summary"):
            st.session_state.quick_question = "Generate a brief weekly summary for leadership."
    with col6:
        if st.button("🎯 Kingpin Progress"):
            st.session_state.quick_question = "How are we tracking against the Kingpin T4W TP90 target of 9.69 days?"
    
    st.markdown("---")
    
    for message in st.session_state.chat_history:
        role_class = "user-message" if message["role"] == "user" else "ai-message"
        icon = "🧑" if message["role"] == "user" else "🤖"
        st.markdown(f'<div class="chat-message {role_class}">{icon} {message["content"]}</div>', unsafe_allow_html=True)
    
    user_input = st.chat_input("Ask a question...")
    
    if "quick_question" in st.session_state:
        user_input = st.session_state.quick_question
        del st.session_state.quick_question
    
    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with st.spinner("Thinking..."):
            response = get_ai_response(user_input, data_context)
        st.session_state.chat_history.append({"role": "assistant", "content": response})
        st.rerun()
    
    if st.button("🗑️ Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()

# =============================================================================
# PAGE: CASE EXPLORER
# =============================================================================

elif page == "Case Explorer":
    st.markdown("## 📋 Case Explorer")
    st.caption(f"Drill into individual cases | APAC | 2026")
    st.markdown("---")
    
    # Check if we have a filter from WBR Generator
    if st.session_state.explorer_filter_dev_id:
        st.success(f"🔍 Filtered to case: **{st.session_state.explorer_filter_dev_id}**")
        if st.button("❌ Clear Filter", help="Show all cases"):
            st.session_state.explorer_filter_dev_id = None
            st.rerun()
        st.markdown("---")
    
    case_filter = st.radio("Case Type", ["All Cases", "Internal Ops Only"], horizontal=True)
    
    if case_filter == "Internal Ops Only":
        cases = get_internal_ops_by_sites(df, selected_sites)
    else:
        cases = get_all_cases_by_sites(df, selected_sites)
    
    if len(cases) == 0:
        st.warning("No data available.")
        st.stop()
    
    # Apply dev_id filter if set from WBR Generator
    if st.session_state.explorer_filter_dev_id:
        filtered = cases[cases['dev_id'] == st.session_state.explorer_filter_dev_id].copy()
        if len(filtered) == 0:
            st.warning(f"Case {st.session_state.explorer_filter_dev_id} not found in current filter.")
            filtered = cases.copy()
    else:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            weeks = ['All'] + sorted(cases['year_week'].unique().tolist(), reverse=True)
            filter_week = st.selectbox("Week", weeks)
        
        with col2:
            verticals = ['All'] + sorted(cases['dev_reason'].unique().tolist()) if 'dev_reason' in cases.columns else ['All']
            filter_vertical = st.selectbox("Vertical", verticals)
        
        with col3:
            min_ttr = st.number_input("Min TTR (days)", min_value=0.0, value=0.0)
        
        filtered = cases.copy()
        if filter_week != 'All':
            filtered = filtered[filtered['year_week'] == filter_week]
        if filter_vertical != 'All' and 'dev_reason' in filtered.columns:
            filtered = filtered[filtered['dev_reason'] == filter_vertical]
        filtered = filtered[filtered['ttr_overall'] >= min_ttr]
    
    st.markdown(f"**Showing {len(filtered):,} cases**")
    
    if len(filtered) > 0:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("P90", f"{filtered['ttr_overall'].quantile(0.9):.2f}d")
        with col2:
            st.metric("Median", f"{filtered['ttr_overall'].median():.2f}d")
        with col3:
            st.metric("Mean", f"{filtered['ttr_overall'].mean():.2f}d")
        with col4:
            st.metric("Max", f"{filtered['ttr_overall'].max():.2f}d")
    
    st.markdown("---")
    
    # Show detailed view for single case
    if st.session_state.explorer_filter_dev_id and len(filtered) == 1:
        st.markdown("### 📄 Case Details")
        case = filtered.iloc[0]
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Dev ID:** `{case.get('dev_id', 'N/A')}`")
            st.markdown(f"**Seller ID:** `{case.get('seller_id', 'N/A')}`")
            st.markdown(f"**Vertical:** {case.get('dev_reason', 'N/A')}")
            st.markdown(f"**Subreason:** {case.get('dev_subreason', 'N/A')}")
            st.markdown(f"**Site:** {case.get('site', 'N/A')}")
            st.markdown(f"**Classification:** {case.get('case_classification', 'N/A')}")
        
        with col2:
            st.markdown(f"**TTR Overall:** {case.get('ttr_overall', 0):.2f} days")
            st.markdown(f"**TTR SRT:** {case.get('ttr_srt', 0):.2f} days" if pd.notna(case.get('ttr_srt')) else "**TTR SRT:** N/A")
            st.markdown(f"**TTR Outreach:** {case.get('ttr_outreach', 0):.2f} days" if pd.notna(case.get('ttr_outreach')) else "**TTR Outreach:** N/A")
            st.markdown(f"**TTR Escalation:** {case.get('ttr_escalation', 0):.2f} days" if pd.notna(case.get('ttr_escalation')) else "**TTR Escalation:** N/A")
            st.markdown(f"**Close Date:** {case.get('dev_end_date', 'N/A')}")
            st.markdown(f"**Week:** {case.get('year_week', 'N/A')}")
            st.markdown(f"**Supervisor:** {case.get('dev_end_supervisor_id', 'N/A')}")
        
        st.markdown("---")
        st.markdown("### 📋 All Fields")
        st.dataframe(filtered.T, use_container_width=True)
    else:
        # Normal table view
        display_cols = ['dev_id', 'dev_reason', 'site', 'case_classification', 'dev_end_supervisor_id', 'dev_end_date', 'year_week', 'ttr_overall']
        available_cols = [c for c in display_cols if c in filtered.columns]
        
        filtered_display = filtered[available_cols].copy()
        filtered_display['ttr_overall'] = filtered_display['ttr_overall'].apply(lambda x: f"{x:.2f}")
        
        st.dataframe(filtered_display.sort_values('ttr_overall', ascending=False), hide_index=True, use_container_width=True)
    
    csv = filtered.to_csv(index=False)
    st.download_button("📥 Download Data", csv, file_name="ttr_cases_2026.csv", mime="text/csv")

# =============================================================================
# PAGE: DOCUMENTATION
# =============================================================================

elif page == "Documentation":
    st.markdown('<p class="main-header">Documentation</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Business Logic, Metrics and Technical Reference</p>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    st.markdown("""
## Quick Reference

### Key Metrics
| Metric | Definition | Target |
|--------|------------|--------|
| **T4W TP90** | 90th percentile TTR, trailing 4 weeks, ALL cases | 9.69 days (Kingpin) |
| **T4W DART P90** | 90th percentile TTR, trailing 4 weeks, Internal Ops only | 7.5 days (Ceiling) |

### Case Classification
| Type | Definition | Percentage |
|------|------------|------------|
| **Internal Ops** | Cases without escalation_outcome or ttr_escalation | ~86% |
| **External Hold** | Cases with escalation_outcome OR ttr_escalation > 0 | ~14% |

### WBR Generator - NEW vs RECURRING Outliers
| Type | Definition | Action |
|------|------------|--------|
| **NEW** | P90+ cases closed in CURRENT week | Need investigation |
| **RECURRING** | P90+ cases from prior weeks still in T4W | Already dived - no action |

This prevents re-investigating cases that were already analyzed in previous weeks!

### Headcount Automation Status
| Role | Status |
|------|--------|
| HC2: Closed Deviation | Automated via WBR Generator |
| HC3: Defect Tracker | Partially automated (defect flagging) |
| HC1: Open Deviation | Pending |
    """)

# =============================================================================
# FOOTER
# =============================================================================

st.markdown("---")
st.caption(f"TTR AI Workspace v2.2 | 2026 Data | APAC (SIN + HND) | T4W: {t4w_weeks_str}")


