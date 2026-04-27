# TTR AI Workspace - Quick Start Guide

## 🚀 How to Run the App

### Step 1: Open Command Prompt
- Press `Windows + R`
- Type `cmd` and press Enter

### Step 2: Navigate to the App Folder
```
cd C:\Users\dangjodi\Desktop\AHA\TTR\TTR_AI_Workspace
```

### Step 3: Run the App
```
py -m streamlit run app.py
```

### Step 4: Open Browser
- The app should auto-open at: **http://localhost:8501**
- If not, copy/paste that URL into your browser

---

## 📝 Testing the WBR Generator

### What to Test:
Click **📝 WBR Generator** in the sidebar.

### Key Features to Validate:

| Feature | What to Check |
|---------|---------------|
| **Week Selector** | Can you select different weeks? |
| **Week Stats** | Do TP90, Avg TTR, Cases, External % look correct? |
| **WoW Changes** | Are the +/- WoW values accurate? |
| **🚀 Generate WBR Narrative** | Click it - does the narrative make sense? |
| **🆕 NEW Outliers** | Are these truly cases closed THIS week? |
| **🔄 RECURRING Outliers** | Are these cases from prior weeks? |
| **Potential Defects** | Are Internal Ops outliers flagged correctly? |

---

## 🆕 vs 🔄 Outlier Logic (IMPORTANT!)

**The app now separates outliers into:**

| Type | Meaning | Action Needed |
|------|---------|---------------|
| **🆕 NEW** | Closed in the selected week, P90+ outlier | ✅ Investigate |
| **🔄 RECURRING** | Closed in prior weeks, still in T4W window | ❌ Already dived last week |

**This prevents re-investigating the same cases every week!**

---

## ✅ Validation Checklist

Please check these and provide feedback:

### Metrics Accuracy
- [ ] T4W TP90 matches your manual calculation
- [ ] Weekly TP90 values are correct
- [ ] WoW changes (+/-) are accurate
- [ ] Case counts match source data

### Outlier Logic
- [ ] NEW outliers are truly from the selected week
- [ ] RECURRING outliers are from prior T4W weeks
- [ ] P90 threshold is calculated correctly

### WBR Narrative
- [ ] Key drivers section is useful
- [ ] Outlier summary is accurate
- [ ] Action items make sense
- [ ] Narrative is ready for WBR (or needs edits?)

### Missing Features
- [ ] What else would be helpful?
- [ ] What's confusing or unclear?
- [ ] Any incorrect logic?

---

## 🔄 How to Refresh Data

1. Replace `data/2026 Closed Deviation WW.csv` with new data
2. Click **🔄 Refresh Data** button in sidebar
3. Data updates automatically

---

## ❓ Questions or Issues?

Contact: [Your Name/Alias]

---

## 📊 Other Pages to Explore

| Page | Purpose |
|------|---------|
| **📈 TP90 Dashboard** | Kingpin goal tracking (T4W) |
| **📊 DART P90 Dashboard** | Internal Ops performance |
| **👥 Manager Scorecard** | Performance by supervisor |
| **📁 Vertical Analysis** | Performance by dev_reason |
| **📋 Case Explorer** | Drill into individual cases |

---

**Thank you for testing! Your feedback will help us automate more of the manual work.** 🎯
