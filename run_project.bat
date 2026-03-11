@echo off
cd /d %~dp0

echo ======================================
echo Running FAF Freight Data Warehouse ETL
echo ======================================

python -m etl.run_all
if errorlevel 1 (
    echo.
    echo Pipeline failed.
    exit /b 1
)

echo.
echo Pipeline completed successfully.
pause