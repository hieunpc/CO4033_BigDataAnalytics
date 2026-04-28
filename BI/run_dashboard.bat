@echo off
REM Streamlit Dashboard Launcher for BRFSS BI

echo Using system Python...
set PYTHON_EXE=C:\Users\hongk\AppData\Local\Programs\Python\Python314\python.exe

echo Checking streamlit installation...
%PYTHON_EXE% -c "import streamlit" 2>nul
if %errorlevel% neq 0 (
    echo Installing streamlit...
    %PYTHON_EXE% -m pip install streamlit plotly
)

echo Starting Streamlit dashboard...
%PYTHON_EXE% -m streamlit run bi_dashboard.py

pause