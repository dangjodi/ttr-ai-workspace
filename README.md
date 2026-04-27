# 🎯 TTR AI Workspace

Interactive dashboard for DART P90 monitoring with AI-powered insights.

## Features

- **📊 Dashboard**: Real-time DART P90 trending with status indicators
- **👥 Manager Scorecard**: Performance by supervisor with color-coded rankings
- **📁 Vertical Analysis**: DART P90 breakdown by dev_reason
- **🤖 AI Chat**: Ask questions about your data in natural language
- **📋 Case Explorer**: Drill into individual cases with filters

## Quick Start

### 1. Install Dependencies

```bash
cd TTR_AI_Workspace
pip install -r requirements.txt
```

### 2. Add Your Data

Place your closed deviation CSV file in the `data` folder:
```
TTR_AI_Workspace/
└── data/
    └── 2026 Closed Deviation WW.csv
```

### 3. Run the App

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

## Optional: Enable AI Chat

To enable full AI capabilities, set your Anthropic API key:

**Windows:**
```bash
set ANTHROPIC_API_KEY=your-key-here
streamlit run app.py
```

**Mac/Linux:**
```bash
export ANTHROPIC_API_KEY=your-key-here
streamlit run app.py
```

## Data Requirements

The app expects a CSV with these columns:
- `dev_id`: Deviation ID
- `dev_end_date`: Resolution date
- `dev_end_site_name`: Site (SIN, HND, etc.)
- `dev_end_supervisor_id`: Manager/Supervisor ID
- `dev_reason`: Vertical (Order Performance, AHR, etc.)
- `ttr_overall`: Total TTR in days
- `ttr_srt`, `ttr_outreach`: Stage-level TTR
- `escalation_outcome`: For classification
- `ttr_stage`: For classification

## Targets

| Status | DART P90 | Meaning |
|--------|----------|---------|
| 🟢 Green | ≤6.0 days | Exceeding - Strong performance |
| 🟡 Yellow | 6.1-7.5 days | Acceptable - On track |
| 🔴 Red | >7.5 days | At Risk - Immediate action needed |

## File Structure

```
TTR_AI_Workspace/
├── app.py              # Main Streamlit application
├── config.py           # Configuration and targets
├── data_loader.py      # Data loading and ETL functions
├── ai_chat.py          # AI chat module
├── requirements.txt    # Python dependencies
├── README.md           # This file
└── data/
    └── 2026 Closed Deviation WW.csv
```

## Screenshots

*(Add screenshots after first run)*

---

Built with ❤️ for APAC AHA Operations
