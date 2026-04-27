@echo off
echo ========================================
echo    TTR AI Workspace - Starting...
echo ========================================
echo.

cd /d "%~dp0"

echo Checking dependencies...
pip install -r requirements.txt -q

echo.
echo Starting TTR AI Workspace...
echo.
echo Dashboard will open at: http://localhost:8501
echo Press Ctrl+C to stop
echo.

streamlit run app.py

pause
