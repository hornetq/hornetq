@ echo off
setlocal ENABLEDELAYEDEXPANSION
set JBM_HOME=..
IF "a%1"== "a" ( 
set CONFIG_DIR=%JBM_HOME%\config\stand-alone\non-clustered
) ELSE (
SET CONFIG_DIR=%1
)
dir >> %CONFIG_DIR%\STOP_ME