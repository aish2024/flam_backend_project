#!/usr/bin/env bash
set -euo pipefail

echo "== QueueCTL Demo =="
python queuectl.py config get
echo "Enqueue jobs..."
python queuectl.py enqueue '{"id":"fast1","command":"echo Hello World"}'
python queuectl.py enqueue '{"id":"slow1","command":"sleep 2"}'
python queuectl.py enqueue '{"id":"fail1","command":"bash -c \"exit 1\"","max_retries":2}'

echo "Start workers (2)"
python queuectl.py worker start --count 2
sleep 1
python queuectl.py status

echo "Wait for processing..."
sleep 6

echo "Status:"
python queuectl.py status

echo "Completed:"
python queuectl.py list --state completed

echo "Failed:"
python queuectl.py list --state failed

echo "DLQ:"
python queuectl.py dlq list

echo "Stopping workers..."
python queuectl.py worker stop
echo "Done."
