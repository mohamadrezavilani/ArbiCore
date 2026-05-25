# deploy.ps1
$server = "ubuntu@95.38.165.226"
$remotePath = "/home/ubuntu/ArbiCore/ArbiCore/"

Write-Host "🚀 Deploying code to server..." -ForegroundColor Cyan

# Copy all files and folders, excluding .env, .venv, __pycache__, .git, and any temporary files
scp -r -o "Compression=yes" `
    *.py `
    requirements.txt `
    pyproject.toml `
    app/ `
    tests/ `
    scripts/ `
    $server":"$remotePath

Write-Host "🔄 Restarting service..." -ForegroundColor Cyan
ssh $server "sudo systemctl restart arbicore"

Write-Host "✅ Deployment complete!" -ForegroundColor Green