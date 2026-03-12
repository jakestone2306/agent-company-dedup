import os
import threading
import traceback
from datetime import datetime
from flask import Flask, request, jsonify
from agent import run_dedup

app = Flask(__name__)

run_state = {
    "status": "idle",
    "last_run": None,
    "last_result": None,
    "last_error": None,
    "dry_run": None,
    "progress": None,
}

def run_in_background(dry_run):
    run_state["status"] = "running"
    run_state["last_run"] = datetime.utcnow().isoformat()
    run_state["dry_run"] = dry_run
    run_state["progress"] = {"phase": "starting", "merges_done": 0, "merges_failed": 0, "total_to_merge": 0}
    try:
        result = run_dedup(dry_run=dry_run, progress=run_state["progress"])
        run_state["status"] = "success"
        run_state["last_result"] = {k: v for k, v in result.items() if k != "details"}
        run_state["last_error"] = None
        run_state["progress"] = None
    except Exception as e:
        run_state["status"] = "error"
        run_state["last_error"] = traceback.format_exc()

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agent": "company-dedup-agent", "run_state": run_state})

@app.route("/status", methods=["GET"])
def status():
    return jsonify(run_state)

@app.route("/dry-run", methods=["POST"])
def dry_run():
    if run_state["status"] == "running":
        return jsonify({"status": "already_running"}), 409
    t = threading.Thread(target=run_in_background, args=(True,))
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "Dry run started — no changes will be made. Check /status."})

@app.route("/run", methods=["POST"])
def run():
    if run_state["status"] == "running":
        return jsonify({"status": "already_running"}), 409
    t = threading.Thread(target=run_in_background, args=(False,))
    t.daemon = True
    t.start()
    return jsonify({"status": "started", "message": "Dedup started. Check /status for progress."})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
