@echo off

rem   Run Redis server, ZX SDK for hangqing, Calc nums with python

rem redis-server
start "redis-server" /D "E:\000_BAK\redis-2.4.5-win32-win64\64bit" "redis-server.exe"  "redis.conf"

rem zx sdk for hq
start "zx sdk for hq" /D "E:\project\vstudio\hqtdf-win32\install\bin" "hqtdf-win32.exe"

rem Calc nums with python
start "Calc nums" /D "E:\project\pychram\trade_scrips\work\everyminute" "python"  "gen-cur-minute-macd-num-data.py"


rem for /f "tokens=2 delims= " %%a in ('tasklist ^| find "BitComet.exe"') do (if %%a neq "" ntsd -c q -p %%a)
rem taskkill /f /t /im  redis-server.exe
rem taskkill /f /t /im  hqtdf-win32.exe