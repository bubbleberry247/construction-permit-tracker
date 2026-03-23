# setup_task_scheduler.ps1
# Windowsタスクスケジューラに自動パイプラインタスクを登録する
# 管理者権限で実行すること: powershell -ExecutionPolicy Bypass -File scripts\setup_task_scheduler.ps1

$TaskName = "建設業許可証管理_自動パイプライン"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ScriptPath  = Join-Path $ProjectRoot "scripts\run_pipeline.bat"

# 既存タスクを削除（再登録用）
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

$Action  = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$ScriptPath`""
$Trigger = New-ScheduledTaskTrigger -Daily -At "07:00AM"
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action   $Action `
    -Trigger  $Trigger `
    -Settings $Settings `
    -Description "建設業許可証管理システム: Gmail受信→OCR→Sheets登録を毎朝7時に自動実行" `
    -RunLevel Highest

Write-Host "タスク登録完了: $TaskName"
Write-Host "次回実行: 翌朝 07:00"
Write-Host ""
Write-Host "確認コマンド:"
Write-Host "  Get-ScheduledTask -TaskName '$TaskName' | Select-Object TaskName, State"
Write-Host "手動実行:"
Write-Host "  Start-ScheduledTask -TaskName '$TaskName'"
