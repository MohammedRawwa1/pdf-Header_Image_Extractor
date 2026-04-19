# Read BOT_TOKEN from .env and call getWebhookInfo
$envFile = Join-Path (Get-Location) '.env'
if (-not (Test-Path $envFile)) { Write-Error ".env not found"; exit 1 }
$bot = Get-Content $envFile | Where-Object { $_ -match '^\s*BOT_TOKEN\s*=' } | ForEach-Object { ($_ -split '=',2)[1].Trim() } | Select-Object -First 1
if (-not $bot) { Write-Error "BOT_TOKEN not found in .env"; exit 1 }
try {
    $res = Invoke-RestMethod -Uri "https://api.telegram.org/bot$bot/getWebhookInfo" -Method Get -ErrorAction Stop
    $res | ConvertTo-Json -Depth 5
} catch {
    Write-Error "getWebhookInfo failed: $($_.Exception.Message)"
    exit 1
}