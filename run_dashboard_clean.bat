@echo off
REM Streamlit Dashboard Launcher for BRFSS BI
REM Sử dụng virtual environment (.venv) để chạy dashboard

echo Activating virtual environment...
call .venv\Scripts\activate

echo Starting Streamlit dashboard...
python -m streamlit run bi_dashboard_clean.py

pause
