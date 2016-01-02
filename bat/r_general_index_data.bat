@echo off

rem Calc nums with python
rem start "Get today data" /D  "E:\project\pychram\trade_scrips\work\everyday" "python"  "http-get-current-day-stock-data.py"

rem  gener today index data
start "General today index data" /D "E:\project\pychram\trade_scrips\work\everyday" "python"  "read-everyday-csvfile-and-append-to-index-data.py"

