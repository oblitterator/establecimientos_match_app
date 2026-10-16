@echo off
setlocal

REM Ir a la carpeta del proyecto (donde esta este .bat)
cd /d "%~dp0"

echo ==========================================
echo   Iniciando Match REFES / Georef INDEC
echo ==========================================
echo.

REM Redirigir temporales/cache al proyecto para evitar fallos por C: sin espacio
if not exist ".tmp" mkdir ".tmp"
if not exist ".pip-cache" mkdir ".pip-cache"
set "TMP=%CD%\.tmp"
set "TEMP=%CD%\.tmp"
set "PIP_CACHE_DIR=%CD%\.pip-cache"
set "PIP_NO_CACHE_DIR=1"

REM Usar el Python del entorno virtual si existe
if exist "venv\Scripts\python.exe" (
    echo Usando entorno virtual: venv\Scripts\python.exe
    "venv\Scripts\python.exe" app.py
) else (
    echo No se encontro el entorno virtual.
    echo Intentando crearlo con setup_venv.ps1...
    echo.

    if exist "setup_venv.ps1" (
        powershell -ExecutionPolicy Bypass -File "setup_venv.ps1"
        if errorlevel 1 (
            echo.
            echo Error al ejecutar setup_venv.ps1.
            echo Intentando iniciar con Python del sistema...
            python app.py
        ) else (
            if exist "venv\Scripts\python.exe" (
                echo.
                echo Entorno virtual creado correctamente.
                echo Iniciando app con venv...
                "venv\Scripts\python.exe" app.py
            ) else (
                echo.
                echo El setup finalizo, pero no se encontro venv\Scripts\python.exe.
                echo Intentando iniciar con Python del sistema...
                python app.py
            )
        )
    ) else (
        echo No se encontro setup_venv.ps1. Usando Python del sistema...
        python app.py
    )
)

if errorlevel 1 (
    echo.
    echo Ocurrio un error al iniciar la aplicacion.
    echo Verifica dependencias o ejecuta setup_venv.ps1
)

echo.
echo Presiona una tecla para cerrar esta ventana...
pause >nul
