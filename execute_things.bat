@echo off

echo Starting Dramatiq worker...
start cmd /k "dramatiq script_dramatiq_worker --threads 2"

echo Running the script...
python script_dramatiq_worker.py

pause