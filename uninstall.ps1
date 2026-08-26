# Freebuff Endpoint — uninstaller (works standalone OR from inside the repo)
#
# Same energy as the installer: download & run from anywhere.
#
#   Stop autostart + gateway, keep files:
#     powershell -ExecutionPolicy Bypass -File uninstall.ps1
#
#   Complete removal (also deletes the install folder incl. config/tokens):
#     powershell -ExecutionPolicy Bypass -File uninstall.ps1 -Full
#
# One-line remote versions are in the README.

param(
  [switch]$Full,
  [string]$Dir = ""
)

$ErrorActionPreference = "SilentlyContinue"

# Locate the install: prefer the folder we're sitting in, else the default.
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if ($Dir) { $Install = $Dir }
elseif ((Test-Path (Join-Path $here "package.json")) -and (Test-Path (Join-Path $here "src"))) { $Install = $here }
else { $Install = Join-Path $env:USERPROFILE "freebuff-endpoint" }

$Startup = [Environment]::GetFolderPath('Startup')
$LauncherVbs = Join-Path $Startup "freebuff-endpoint.vbs"

Write-Host "== Freebuff Endpoint uninstaller ==" -ForegroundColor Cyan
Write-Host "Target install: $Install"

# 1. Remove the auto-start launcher FIRST so nothing respawns.
Remove-Item -Force $LauncherVbs -ErrorAction SilentlyContinue
if (Test-Path $LauncherVbs) { Write-Host "WARN: could not remove $LauncherVbs" -ForegroundColor Yellow } else { Write-Host "Autostart removed." }

# 2. Kill the watchdog (cmd.exe hosting runner.cmd) — otherwise it resurrects node.
Get-CimInstance Win32_Process -Filter "Name='cmd.exe' OR Name='wscript.exe'" |
  Where-Object { $_.CommandLine -match 'runner\.cmd|freebuff-endpoint' } |
  ForEach-Object {
    Write-Host "Stopping watchdog (pid $($_.ProcessId))..."
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
  }

# 3. Kill ONLY the gateway's node process (by port) — never other node apps.
$conns = Get-NetTCPConnection -LocalPort 8090 -State Listen -ErrorAction SilentlyContinue
if ($conns) {
  $pids = $conns.OwningProcess | Sort-Object -Unique
  foreach ($p in $pids) {
    Write-Host "Stopping gateway node process (pid $p)..."
    Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
  }
} else {
  Write-Host "Gateway not currently listening."
}

# Belt-and-suspenders: scheduled task from older installs.
Unregister-ScheduledTask -TaskName "FreebuffEndpoint" -Confirm:$false -ErrorAction SilentlyContinue

Start-Sleep -Seconds 2
$stillUp = Get-NetTCPConnection -LocalPort 8090 -State Listen -ErrorAction SilentlyContinue
if ($stillUp) {
  Write-Host "WARN: something still listens on 8090 (maybe another app)." -ForegroundColor Yellow
} else {
  Write-Host "Gateway stopped."
}

# 4. Optional: full removal.
if ($Full) {
  if (Test-Path $Install) {
    # Never delete unless it really looks like our install.
    $looksRight = (Test-Path (Join-Path $Install "src\server.js")) -or (Test-Path (Join-Path $Install "package.json"))
    if (-not $looksRight) {
      Write-Host "Refusing to delete '$Install' — it doesn't look like a Freebuff Endpoint install." -ForegroundColor Red
      exit 1
    }
    Remove-Item -Recurse -Force $Install
    if (Test-Path $Install) { Write-Host "WARN: folder could not be fully removed (a file may be locked)." -ForegroundColor Yellow }
    else { Write-Host "Deleted $Install (config, tokens, logs, cache included)." }
  } else {
    Write-Host "Nothing to delete at $Install."
  }
  Write-Host ""
  Write-Host "FULLY REMOVED. Zero traces left on this machine." -ForegroundColor Green
} else {
  Write-Host ""
  Write-Host "STOPPED. Files remain at $Install (tokens/config kept)." -ForegroundColor Green
  Write-Host "Remove completely: add -Full   |   Restart later: re-run the installer."
}
