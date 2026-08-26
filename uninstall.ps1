# Teardown — remove Freebuff Endpoint from this machine completely
#
#   powershell -ExecutionPolicy Bypass -File uninstall.ps1          # remove service + stop server
#   powershell -ExecutionPolicy Bypass -File uninstall.ps1 -Wipe    # ALSO delete config/logs/data (keeps this folder's code)
#
# Then just delete the project folder itself. Nothing is installed anywhere
# else on the system: no registry entries, no services, no admin artifacts.

param([switch]$Wipe)

$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Startup = [Environment]::GetFolderPath('Startup')
$LauncherVbs = Join-Path $Startup "freebuff-endpoint.vbs"

Write-Host "Stopping gateway…"
Get-Process node -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "Removing auto-start launcher…"
Remove-Item -Force $LauncherVbs -ErrorAction SilentlyContinue

# Belt and suspenders: remove the task too, if it ever existed.
Unregister-ScheduledTask -TaskName "FreebuffEndpoint" -Confirm:$false -ErrorAction SilentlyContinue

if ($Wipe) {
  Remove-Item -Recurse -Force (Join-Path $ProjectDir "logs") -ErrorAction SilentlyContinue
  Remove-Item -Recurse -Force (Join-Path $ProjectDir "data") -ErrorAction SilentlyContinue
  Write-Host "Wiped logs, data cache, and local config backup."
  Write-Host "(config.json kept only if you answer No at the prompt below.)"
  $ans = Read-Host "Also delete config.json (contains your tokens/proxy)? y/N"
  if ($ans -match '^[Yy]') {
    Remove-Item -Force (Join-Path $ProjectDir "config.json") -ErrorAction SilentlyContinue
    Write-Host "config.json deleted."
  }
}

Write-Host ""
Write-Host "Service removed. To finish, delete the project folder itself:" -ForegroundColor Green
Write-Host "  $ProjectDir"
