# PowerShell demo for QueueCTL (PowerShell-safe)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Output "== QueueCTL PowerShell Demo =="
python queuectl.py config get
Write-Output "Enqueue jobs..."
# Use double quotes and escape inner double quotes for PowerShell
python queuectl.py enqueue "{""id"":""fast1"",""command"":""echo Hello World""}"
python queuectl.py enqueue "{""id"":""slow1"",""command"":""python -c \""import time; time.sleep(2); print('done')\""""}"
python queuectl.py enqueue "{""id"":""fail1"",""command"":""python -c \""import sys; sys.exit(1)\"""",""max_retries"":2}"

Write-Output "Start workers (2)"
Start-Process -FilePath python -ArgumentList "queuectl.py worker start --count 2" -NoNewWindow
Start-Sleep -Seconds 1
python queuectl.py status

Write-Output "Wait for processing..."
Start-Sleep -Seconds 6

Write-Output "`nStatus:"
python queuectl.py status

Write-Output "`nCompleted:"
python queuectl.py list --state completed

Write-Output "`nFailed:"
python queuectl.py list --state failed

Write-Output "`nDLQ:"
python queuectl.py dlq list

Write-Output "`nStopping workers..."
python queuectl.py worker stop
Write-Output "Done."
