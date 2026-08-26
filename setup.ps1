# Freebuff Endpoint — one-shot installer (run from anywhere)
#
#   powershell -ExecutionPolicy Bypass -File setup.ps1
#
# Clones/updates the repo, installs deps, installs the hidden auto-start
# service, and starts the gateway. Re-run safely at any time.

$ErrorActionPreference = "Stop"
$Repo = "https://github.com/lonewolfmasanaga/freebuff-endpoint.git"
$Dest = Join-Path $env:USERPROFILE "freebuff-endpoint"

Write-Host "== Freebuff Endpoint installer ==" -ForegroundColor Cyan

# 1. Node.js present?
if (-not (Get-Command node.exe -ErrorAction SilentlyContinue)) {
  Write-Host "Node.js not found. Opening the download page..." -ForegroundColor Yellow
  Write-Host "Install the LTS from https://nodejs.org , then run this script again."
  Start-Process "https://nodejs.org"
  exit 1
}
Write-Host ("Node: " + (node --version))

# 2. Get or update the code.
if (Test-Path (Join-Path $Dest ".git")) {
  Write-Host "Updating existing install at $Dest"
  Push-Location $Dest
  git pull --ff-only 2>$null
} else {
  Write-Host "Cloning to $Dest"
  git clone $Repo $Dest
  if ($LASTEXITCODE -ne 0) { throw "git clone failed" }
  Push-Location $Dest
}

# 3. Dependencies.
Write-Host "Installing dependencies..."
npm install --no-fund --no-audit
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "npm install failed" }

# 4. Default config on first run.
if (-not (Test-Path "config.json")) {
  Copy-Item config.example.json config.json
  Write-Host "Created default config.json"
}
Pop-Location

# 5. Hidden auto-start service + start now.
Push-Location $Dest
powershell -NoProfile -ExecutionPolicy Bypass -File install-windows.ps1
Pop-Location

Write-Host ""
Write-Host "DONE." -ForegroundColor Green
Write-Host "Dashboard:   http://127.0.0.1:8090/"
Write-Host "Install at:  $Dest"
Write-Host "Next step:   open the dashboard and paste your Freebuff auth token."
