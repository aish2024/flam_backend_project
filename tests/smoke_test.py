import subprocess, json, time, os, sys, pathlib

BASE = pathlib.Path(__file__).resolve().parents[1]

def run(cmd):
    return subprocess.check_output(cmd, cwd=BASE).decode()

def main():
    # fresh db
    db = BASE / "queue.db"
    if db.exists():
        db.unlink()

    print("Enqueue ok job")
    run([sys.executable, "queuectl.py", "enqueue", '{"id":"test_ok","command":"echo ok"}'])
    print("Start worker")
    run([sys.executable, "queuectl.py", "worker", "start", "--count", "1"])
    time.sleep(2)
    out = run([sys.executable, "queuectl.py", "list", "--state", "completed"])
    data = json.loads(out)
    assert any(j["id"]=="test_ok" for j in data), "Job did not complete"
    print("Smoke test passed.")

if __name__ == "__main__":
    main()
