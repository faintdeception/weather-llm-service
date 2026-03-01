param(
  [string]$PythonPath,
  [string]$WorkingDir = "$PSScriptRoot",
  [string]$EnvFile    = "$PSScriptRoot\.env",
  [string]$LogDir     = "$env:ProgramData\weather-llm-service\logs",
  [int]$KeepLogs      = 30
)

# Default to local venv Python if available, else fall back to system 3.12
if (-not $PythonPath) {
  $venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
  if (Test-Path $venvPython) {
    $PythonPath = $venvPython
  }
  else {
    $PythonPath = "C:\\Python312\\python.exe"
  }
}

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[run_job] Starting" -ForegroundColor Cyan
Write-Host "[run_job] PythonPath=$PythonPath" -ForegroundColor Cyan
Write-Host "[run_job] WorkingDir=$WorkingDir" -ForegroundColor Cyan
Write-Host "[run_job] EnvFile=$EnvFile (exists: $([IO.File]::Exists($EnvFile)))" -ForegroundColor Cyan
Write-Host "[run_job] LogDir=$LogDir" -ForegroundColor Cyan
Write-Host "[run_job] KeepLogs=$KeepLogs" -ForegroundColor Cyan

# Ensure log directory exists
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# Load env vars from .env (simple KEY=VALUE, ignore comments/blank lines)
if (Test-Path $EnvFile) {
  Write-Host "[run_job] Loading env vars from $EnvFile" -ForegroundColor Cyan
  Get-Content $EnvFile |
    Where-Object { $_ -match '^\s*[^#\s]+' } |
    ForEach-Object {
      $parts = $_ -split '=', 2
      if ($parts.Count -eq 2) {
        $name = $parts[0].Trim()
        $val  = $parts[1].Trim()
        [Environment]::SetEnvironmentVariable($name, $val)
      }
    }
}
else {
  Write-Host "[run_job] Env file not found at $EnvFile" -ForegroundColor Yellow
}

# Ensure Python uses UTF-8 for stdout/stderr to avoid intermittent logging failures
[Environment]::SetEnvironmentVariable("PYTHONUTF8", "1")
[Environment]::SetEnvironmentVariable("PYTHONIOENCODING", "utf-8")

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $LogDir "run_$timestamp.log"
"[run_job] Log start $timestamp" | Out-File -FilePath $logPath -Encoding UTF8

Push-Location $WorkingDir
try {
  Write-Host "[run_job] Invoking: $PythonPath -m app.services.scheduled_task" -ForegroundColor Cyan
  $output = & $PythonPath -m app.services.scheduled_task 2>&1 | Tee-Object -FilePath $logPath -Append
  $exitCode = $LASTEXITCODE
  Write-Host "[run_job] Python exit code: $exitCode" -ForegroundColor Cyan
  if ($exitCode -ne 0) {
    Write-Host "[run_job] Python stderr/stdout:" -ForegroundColor Yellow
    $output | ForEach-Object { Write-Host $_ -ForegroundColor Yellow }
    throw "Python exited with code $exitCode"
  }
}
catch {
  Write-Host "[run_job] Exception in runner: $_" -ForegroundColor Red
  "[run_job] Exception in runner: $_" | Tee-Object -FilePath $logPath -Append | Out-Null
  throw
}
finally {
  Pop-Location
}

# Rolling retention: keep newest $KeepLogs files, delete older
Get-ChildItem -Path $LogDir -Filter "run_*.log" |
  Sort-Object LastWriteTime -Descending |
  Select-Object -Skip $KeepLogs |
  Remove-Item -Force
