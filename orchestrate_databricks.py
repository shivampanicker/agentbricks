import os
import time
import json
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.jobs import (
    RunSubmitTaskSettings,
    NotebookTask,
    ClusterSpec,
)
from databricks.sdk.service.pipelines import (
    PipelineLibrary,
    NotebookLibrary,
)

# Environment is taken from DATABRICKS_HOST/DATABRICKS_TOKEN (and others) by WorkspaceClient

WS_PATH = "/Workspace/Shared/agentbricks"
LOCAL_FILES = [
    "content_insurance_datasets.py",
    "dlt_pipeline.py",
    "dlt_silver_pipeline.py",
    "claims_agents.py",
    "register_and_serve_agents.py",
]


def upload_notebooks(w: WorkspaceClient) -> None:
    w.workspace.mkdirs(WS_PATH)
    for fname in LOCAL_FILES:
        local_path = Path(fname)
        ws_target = f"{WS_PATH}/{local_path.name}"
        with open(local_path, "rb") as f:
            content = f.read()
        w.workspace.import_(
            path=ws_target,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=content,
            overwrite=True,
        )
        print(f"Imported {fname} -> {ws_target}")


def submit_notebook_run(w: WorkspaceClient, notebook_path: str, parameters: Optional[dict] = None) -> None:
    task = RunSubmitTaskSettings(
        task_key="notebook",
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            base_parameters=parameters or {},
        ),
        new_cluster=ClusterSpec(
            spark_version="13.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=1,
            spark_conf={"spark.databricks.cluster.profile": "singleNode"},
            custom_tags={"ResourceClass": "SingleNode"},
        ),
        timeout_seconds=7200,
    )
    run = w.jobs.submit(run_name=f"run {Path(notebook_path).name}", tasks=[task])
    print(f"Submitted run {run.run_id} for {notebook_path}")
    wait_for_run(w, run.run_id)


def wait_for_run(w: WorkspaceClient, run_id: int) -> None:
    while True:
        r = w.jobs.get_run(run_id)
        state = r.state.life_cycle_state
        result = r.state.result_state
        if state in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
            if result and result != "SUCCESS":
                raise RuntimeError(f"Run {run_id} failed: {result}")
            print(f"Run {run_id} finished: {result}")
            return
        print(f"Run {run_id} state: {state}...")
        time.sleep(15)


def ensure_pipeline(w: WorkspaceClient, name: str, notebook_ws_path: str, catalog: str, target: str) -> str:
    # Try to find existing pipeline id
    existing = [p for p in w.pipelines.list() if p.name == name]
    if existing:
        pipeline_id = existing[0].pipeline_id
        print(f"Updating pipeline {name} ({pipeline_id})")
        w.pipelines.edit(
            pipeline_id=pipeline_id,
            name=name,
            catalog=catalog,
            target=target,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=notebook_ws_path))],
            development=True,
            continuous=False,
            channel="CURRENT",
            photon=True,
        )
        return pipeline_id

    print(f"Creating pipeline {name}")
    p = w.pipelines.create(
        name=name,
        catalog=catalog,
        target=target,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=notebook_ws_path))],
        development=True,
        continuous=False,
        channel="CURRENT",
        photon=True,
    )
    return p.pipeline_id


def start_and_wait_pipeline(w: WorkspaceClient, pipeline_id: str) -> None:
    w.pipelines.start_update(pipeline_id)
    while True:
        st = w.pipelines.get(pipeline_id)
        state = st.state
        print(f"Pipeline {pipeline_id} state: {state}")
        if state == "IDLE":
            print(f"Pipeline {pipeline_id} IDLE (completed update)")
            return
        time.sleep(20)


def main() -> None:
    w = WorkspaceClient()

    print("Uploading notebooks to workspace...")
    upload_notebooks(w)

    print("Running dataset generation notebook...")
    submit_notebook_run(w, f"{WS_PATH}/content_insurance_datasets.py")

    print("Creating/Starting Bronze DLT pipeline...")
    bronze_id = ensure_pipeline(
        w,
        name="content_insurance_dlt_bronze",
        notebook_ws_path=f"{WS_PATH}/dlt_pipeline.py",
        catalog="suncorp_catalog",
        target="suncorp_bronze_schema",
    )
    start_and_wait_pipeline(w, bronze_id)

    print("Creating/Starting Silver DLT pipeline...")
    silver_id = ensure_pipeline(
        w,
        name="content_insurance_dlt_silver",
        notebook_ws_path=f"{WS_PATH}/dlt_silver_pipeline.py",
        catalog="suncorp_catalog",
        target="suncorp_silver_schema",
    )
    start_and_wait_pipeline(w, silver_id)

    print("Running claims agents notebook...")
    submit_notebook_run(w, f"{WS_PATH}/claims_agents.py")

    print("Registering models and provisioning serving endpoints...")
    submit_notebook_run(w, f"{WS_PATH}/register_and_serve_agents.py")

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()
