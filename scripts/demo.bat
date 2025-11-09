@echo off
echo == QueueCTL Demo ==
python queuectl.py config get
echo Enqueue jobs...
python queuectl.py enqueue "{\"id\":\"fast1\",\"command\":\"echo Hello World\"}"
python queuectl.py enqueue "{\"id\":\"slow1\",\"command\":\"python -c \"\"import time; time.sleep(2); print('done')\"\"\"}"
python queuectl.py enqueue "{\"id\":\"fail1\",\"command\":\"python -c \"\"import sys; sys.exit(1)\"\"\",\"max_retries\":2}"
echo Start workers (2)
python queuectl.py worker start --count 2
timeout /t 1 >NUL
python queuectl.py status
echo Wait for processing...
timeout /t 6 >NUL
echo Status:
python queuectl.py status
echo Completed:
python queuectl.py list --state completed
echo Failed:
python queuectl.py list --state failed
echo DLQ:
python queuectl.py dlq list
echo Stopping workers...
python queuectl.py worker stop
echo Done.
