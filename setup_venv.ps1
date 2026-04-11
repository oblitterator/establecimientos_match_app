# setup_venv.ps1 — crea el entorno virtual e instala dependencias
Set-Location $PSScriptRoot

if (-not (Test-Path "venv")) {
    Write-Host "Creando entorno virtual..."
    python -m venv venv
}

Write-Host "Activando venv e instalando dependencias..."
& "venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
& "venv\Scripts\pip.exe" install -r requirements.txt

Write-Host ""
Write-Host "Listo. Para correr la app:"
Write-Host "  venv\Scripts\activate"
Write-Host "  cd match_app"
Write-Host "  python app.py"
