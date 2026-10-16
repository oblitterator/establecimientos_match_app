# setup_venv.ps1 — crea el entorno virtual e instala dependencias
Set-Location $PSScriptRoot

$tmpDir = Join-Path $PSScriptRoot ".tmp"
$pipCacheDir = Join-Path $PSScriptRoot ".pip-cache"
New-Item -ItemType Directory -Force -Path $tmpDir *> $null
New-Item -ItemType Directory -Force -Path $pipCacheDir *> $null
$env:TEMP = $tmpDir
$env:TMP = $tmpDir
$env:PIP_CACHE_DIR = $pipCacheDir
$env:PIP_NO_CACHE_DIR = "1"

if (-not (Test-Path "venv")) {
    Write-Host "Creando entorno virtual..."
    python -m venv venv
}

Write-Host "Activando venv e instalando dependencias..."
$pythonVenv = "venv\Scripts\python.exe"

& $pythonVenv -m pip --version *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "pip no esta disponible en el venv. Intentando repararlo con ensurepip..."
    & $pythonVenv -m ensurepip --upgrade
    if ($LASTEXITCODE -ne 0) {
        Write-Error "No se pudo inicializar pip en el entorno virtual."
        exit 1
    }
}

& $pythonVenv -m pip install --upgrade pip --quiet --no-cache-dir
if ($LASTEXITCODE -ne 0) {
    Write-Error "No se pudo actualizar pip en el entorno virtual."
    exit 1
}

& $pythonVenv -m pip install --no-cache-dir -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Error "No se pudieron instalar las dependencias de requirements.txt."
    exit 1
}

Write-Host ""
Write-Host "Listo. Para correr la app:"
Write-Host "  venv\Scripts\activate"
Write-Host "  python app.py"
