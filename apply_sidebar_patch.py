"""
Patch script: Updates sidebar to hide manual upload unless refresh fails
Run once: py apply_sidebar_patch.py
"""

import re

# Read current app.py
with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Old sidebar code to replace (from line 130 to ~191)
old_pattern = r'''# =============================================================================
# SIDEBAR
# =============================================================================

with st\.sidebar:
    st\.markdown\("## TTR AI Workspace"\)
    st\.caption\("APAC \(SIN \+ HND\) \| 2026"\)
    st\.markdown\("---"\)
    
    # Data Refresh Section
    st\.markdown\("### Data Refresh"\)
    
    # Option 1: Auto refresh from DataCentral
    if st\.button\("Refresh from DataCentral", use_container_width=True, type="primary"\):
        with st\.spinner\("Connecting to DataCentral\.\.\."\):
            try:
                from data_refresh import refresh_data
                result = refresh_data\(\)
                
                if result\['success'\]:
                    st\.success\(result\['message'\]\)
                    st\.cache_data\.clear\(\)
                    st\.rerun\(\)
                else:
                    st\.error\(result\['message'\]\)
                    st\.info\("Try manual upload below, or run 'mwinit' in terminal\."\)
            except Exception as e:
                st\.error\(f"Error: \{e\}"\)
                st\.info\("Use manual upload below\."\)
    
    # Option 2: Manual file upload
    uploaded_file = st\.file_uploader\("Or upload CSV manually", type=\['csv'\], help="Download from DataCentral and upload here"\)
    
    if uploaded_file is not None:
        try:
            import pandas as pd
            df_upload = pd\.read_csv\(uploaded_file\)
            
            # Filter to APAC
            site_col = 'site_name' if 'site_name' in df_upload\.columns else 'dev_end_site_name'
            if site_col in df_upload\.columns:
                df_upload = df_upload\[df_upload\[site_col\]\.isin\(\['SIN', 'HND'\]\)\]
            
            # Save to data folder
            save_path = Path\(DATA_DIR\) / CLOSED_DEVIATION_FILE
            df_upload\.to_csv\(save_path, index=False\)
            
            st\.success\(f"Uploaded \{len\(df_upload\):,\} rows"\)
            st\.cache_data\.clear\(\)
            st\.rerun\(\)
        except Exception as e:
            st\.error\(f"Upload error: \{e\}"\)
    
    # Simple cache clear
    if st\.button\("Clear Cache Only", use_container_width=True\):
        st\.cache_data\.clear\(\)
        st\.rerun\(\)'''

# New sidebar code
new_code = '''# =============================================================================
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
                content_upload = uploaded_file.read().decode('utf-8')
                sep = '\\t' if '\\t' in content_upload.split('\\n')[0] else ','
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
        st.cache_data.clear()'''

# Simple string replacement approach
# Find the sidebar section and replace it
lines = content.split('\n')
start_idx = None
end_idx = None

for i, line in enumerate(lines):
    if '# SIDEBAR' in line and '====' in lines[i-1] if i > 0 else False:
        start_idx = i - 1
    if start_idx and 'st.cache_data.clear()' in line and 'rerun' in lines[i+1] if i+1 < len(lines) else False:
        end_idx = i + 2
        break
    if start_idx and 'if st.button("Clear Cache Only"' in line:
        # Find the end of this block
        for j in range(i, min(i+5, len(lines))):
            if 'st.rerun()' in lines[j]:
                end_idx = j + 1
                break
        break

if start_idx and end_idx:
    new_lines = lines[:start_idx] + new_code.split('\n') + lines[end_idx:]
    new_content = '\n'.join(new_lines)
    
    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("✅ Sidebar patched successfully!")
    print("   - Manual upload now hidden by default")
    print("   - Shows only when auto-refresh fails")
else:
    print("❌ Could not locate sidebar section to patch")
    print(f"   start_idx: {start_idx}, end_idx: {end_idx}")
