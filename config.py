"""
TTR AI Workspace - Configuration
"""

# DART P90 Targets
DART_P90_STRETCH = 6.0      # Green threshold
DART_P90_CEILING = 7.5      # Red threshold

# All Sites (for filter)
ALL_SITES = ['SIN', 'HND', 'BCN', 'HYD', 'PHX']

# APAC Sites (default)
APAC_SITES = ['SIN', 'HND']

# Data paths (relative to app.py)
DATA_DIR = "data"
CLOSED_DEVIATION_FILE = "2026 Closed Deviation WW.csv"
OPEN_DEVIATION_FILE = "2026 Open Deviation WW.csv"

# Page config
PAGE_TITLE = "TTR AI Workspace"
PAGE_ICON = "🎯"

# Color scheme
COLORS = {
    'green': '#00C853',
    'yellow': '#FFD600', 
    'red': '#FF1744',
    'primary': '#1E88E5',
    'secondary': '#424242',
    'background': '#FAFAFA'
}

# Status thresholds
def get_status(dart_p90):
    if dart_p90 <= DART_P90_STRETCH:
        return 'Green', COLORS['green']
    elif dart_p90 <= DART_P90_CEILING:
        return 'Yellow', COLORS['yellow']
    else:
        return 'Red', COLORS['red']
