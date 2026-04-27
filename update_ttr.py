"""
Script to replace 'ttr_overall' with 'ttr_ops_overall' in all relevant files.
This aligns the app with WBR/QuickSight calculations.
"""

files_to_update = [
    'data_loader.py',
    'wbr_generator.py',
    'app.py'
]

for filename in files_to_update:
    filepath = f'C:\\Users\\dangjodi\\Desktop\\AHA\\TTR\\TTR_AI_Workspace\\{filename}'
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Count before
    count_before = content.count("'ttr_overall'") + content.count('"ttr_overall"')
    
    # Replace - be careful not to double-replace ttr_ops_overall
    content = content.replace("'ttr_overall'", "'ttr_ops_overall'")
    content = content.replace('"ttr_overall"', '"ttr_ops_overall"')
    
    # Fix any accidental double replacement
    content = content.replace("'ttr_ops_ops_overall'", "'ttr_ops_overall'")
    content = content.replace('"ttr_ops_ops_overall"', '"ttr_ops_overall"')
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f'{filename}: replaced {count_before} occurrences')

print('\nDone! All files updated to use ttr_ops_overall')
