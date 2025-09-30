import os
import time
import json
import base64
import requests
from pathlib import Path

HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")
assert HOST and TOKEN, "Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars"

WS_REPO_PATH = "/Workspace/Repos/shivampanicker/agentbricks"
LOCAL_FILES = [
    "content_insurance_datasets.py",
    "dlt_pipeline.py",
    "dlt_silver_pipeline.py",
    "claims_agents.py",
    "register_and_serve_agents.py",
]

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def api(method: str, path: str, **kwargs):
    url = f"{HOST}{path}"
    r = requests.request(method, url, headers=HEADERS, **kwargs)
    if not r.ok:
        raise RuntimeError(f"API {path} failed: {r.status_code} {r.text}")
    return r.json() if r.text else {}


def workspace_mkdirs(path: str):
    api("POST", "/api/2.0/workspace/mkdirs", json={"path": path})


def workspace_import(local_file: Path, ws_path: str):
    content = local_file.read_bytes()
    b64 = base64.b64encode(content).decode("utf-8")
    api(
        "POST",
        "/api/2.0/workspace/import",
        json={
            "path": ws_path,
            "format": "SOURCE",
            "language": "PYTHON",
            "content": b64,
            "overwrite": True,
        },
    )


def jobs_runs_submit_notebook(notebook_path: str, parameters: dict | None = None):
    payload = {
        "run_name": f"run {Path(notebook_path).name}",
        "tasks": [
            {
                "task_key": "notebook-task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": parameters or {},
                },
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 1,
                    "spark_conf": {"spark.databricks.cluster.profile": "singleNode"},
                    "custom_tags": {"ResourceClass": "SingleNode"},
                },
                "timeout_seconds": 7200,
            }
        ],
    }
    res = api("POST", "/api/2.1/jobs/runs/submit", json=payload)
    run_id = res["run_id"]
    print(f"Submitted run {run_id} for {notebook_path}")
    wait_for_run(run_id)


def wait_for_run(run_id: int):
    while True:
        res = api("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        state = res["state"]["life_cycle_state"]
        result = res["state"].get("result_state")
        if state in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
            if result and result != "SUCCESS":
                raise RuntimeError(f"Run {run_id} failed: {res}")
            print(f"Run {run_id} finished: {result}")
            return
        print(f"Run {run_id} state: {state}...")
        time.sleep(15)


def pipelines_get_by_name(name: str):
    res = api("GET", "/api/2.0/pipelines")
    for p in res.get("statuses", []):
        if p.get("name") == name:
            return p.get("pipeline_id")
    return None


def pipelines_create_or_update_from_file(name: str, config_file: Path):
    cfg = json.loads(config_file.read_text())
    cfg["name"] = name
    existing_id = pipelines_get_by_name(name)
    if existing_id:
        print(f"Updating pipeline {name} ({existing_id})")
        api("PUT", f"/api/2.0/pipelines/{existing_id}", json=cfg)
        return existing_id
    print(f"Creating pipeline {name}")
    res = api("POST", "/api/2.0/pipelines", json=cfg)
    return res["pipeline_id"]


def pipelines_start_and_wait(pipeline_id: str):
    api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", json={})
    while True:
        res = api("GET", f"/api/2.0/pipelines/{pipeline_id}/events")
        statuses = [e.get("message") for e in res.get("events", [])][-5:]
        print("Pipeline status (tail):", statuses)
        # Best-effort poll using updates API
        st = api("GET", f"/api/2.0/pipelines/{pipeline_id}")
        if st.get("state") == "IDLE":
            print(f"Pipeline {pipeline_id} IDLE (completed update)")
            return
        time.sleep(20)


def main():
    print("Uploading notebooks to workspace...")
    workspace_mkdirs(WS_REPO_PATH)
    for fname in LOCAL_FILES:
        local_path = Path(fname)
        ws_path = f"{WS_REPO_PATH}/{local_path.name}"
        workspace_import(local_path, ws_path)
        print(f"Imported {fname} -> {ws_path}")

    print("Running dataset generation notebook...")
    jobs_runs_submit_notebook(f"{WS_REPO_PATH}/content_insurance_datasets.py")

    print("Creating/Starting Bronze DLT pipeline...")
    bronze_id = pipelines_create_or_update_from_file(
        name="content_insurance_dlt_bronze",
        config_file=Path("dlt_pipeline_config.json"),
    )
    pipelines_start_and_wait(bronze_id)

    print("Creating/Starting Silver DLT pipeline...")
    silver_id = pipelines_create_or_update_from_file(
        name="content_insurance_dlt_silver",
        config_file=Path("dlt_silver_pipeline_config.json"),
    )
    pipelines_start_and_wait(silver_id)

    print("Running claims agents notebook...")
    jobs_runs_submit_notebook(f"{WS_REPO_PATH}/claims_agents.py")

    print("Registering models and provisioning serving endpoints...")
    jobs_runs_submit_notebook(f"{WS_REPO_PATH}/register_and_serve_agents.py")

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()
