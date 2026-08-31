# Freebuff Endpoint - 24/7 background service for Windows (no admin needed)
#
# Installs a hidden auto-start launcher into your Startup folder: the gateway
# starts invisibly at every login, restarts itself if it crashes, and logs to
# logs\gateway.log. Manage everything from the dashboard: http://127.0.0.1:8090/
#
# Usage (from the project folder):
#   powershell -ExecutionPolicy Bypass -File install-windows.ps1         # install + start
#   powershell -ExecutionPolicy Bypass -File install-windows.ps1 -Remove # uninstall

param([switch]$Remove)

$TaskName = "FreebuffEndpoint"
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Node = (Get-Command node.exe).Source
$LogDir = Join-Path $ProjectDir "logs"
$Startup = [Environment]::GetFolderPath('Startup')
$LauncherVbs = Join-Path $Startup "freebuff-endpoint.vbs"

if ($Remove) {
  Remove-Item -Force $LauncherVbs -ErrorAction SilentlyContinue
  Get-Process node -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $Node } | Out-Null
  Stop-Process -Name node -Force -ErrorAction SilentlyContinue
  Write-Host "Removed auto-start. Gateway processes stopped."
  Write-Host "(If other Node apps were running, restart them.)"
  exit 0
}

if (-not (Get-Command node.exe -ErrorAction SilentlyContinue)) {
  throw "node.exe not found in PATH - install Node.js first."
}
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# Watchdog: runs node in a loop; if it dies, wait 5s, start again.
$RunnerScript = Join-Path $LogDir "runner.cmd"
$runnerLines = @(
  '@echo off',
  ':loop',
  "cd /d `"$ProjectDir`"",
  "`"$Node`" src\server.js >> `"$LogDir\gateway.log`" 2>&1",
  "echo [%date% %time%] gateway exited (%errorlevel%), restarting in 5s >> `"$LogDir\gateway.log`"",
  'timeout /t 5 /nobreak >nul',
  'goto loop'
)
Set-Content -Path $RunnerScript -Value $runnerLines -Encoding ASCII

# Hidden launcher in the Startup folder (no admin rights required).
$runLine = 'CreateObject("Wscript.Shell").Run """{0}""", 0, False' -f $RunnerScript
$launcherLines = @(
  "' Freebuff Endpoint - hidden auto-start launcher (managed by install-windows.ps1)",
  $runLine
)
Set-Content -Path $LauncherVbs -Value $launcherLines -Encoding ASCII

# Keep gateway.log under ~5MB.
if ((Test-Path "$LogDir\gateway.log") -and ((Get-Item "$LogDir\gateway.log").Length -gt 5MB)) {
  Move-Item -Force "$LogDir\gateway.log" "$LogDir\gateway.log.old"
}

Start-Process wscript.exe -ArgumentList "`"$LauncherVbs`"" -WindowStyle Hidden
Start-Sleep -Seconds 6
try {
  Invoke-RestMethod -Uri "http://127.0.0.1:8090/healthz" -TimeoutSec 5 | Out-Null
  Write-Host ""
  Write-Host "Installed AND running." -ForegroundColor Green
  Write-Host "Dashboard:   http://127.0.0.1:8090/"
  Write-Host "Auto-start:  $LauncherVbs"
} catch {
  Write-Host ""
  Write-Host "Installed, but not answering yet. Check logs\gateway.log" -ForegroundColor Yellow
}
Write-Host ""
Write-Host "Stop now:     Stop-Process -Name node -Force"
Write-Host "Uninstall:    powershell -File install-windows.ps1 -Remove"
