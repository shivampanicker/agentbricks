import os
import json
import mlflow
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
import pandas as pd
from agent_mlflow_pyfunc import AgentPyFuncModel, build_agent_model

UC_CATALOG = os.getenv("UC_CATALOG", "suncorp_catalog")
UC_SCHEMA = os.getenv("UC_SCHEMA", "suncorp_models_schema")
MODEL_PREFIX = os.getenv("MODEL_PREFIX", "agentbricks_claims")
SERVE_SCALE = int(os.getenv("SERVE_SCALE", "1"))

AGENTS = ["triage", "fraud", "settlement", "routing"]


def register_models():
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()

    # Example input to build signature
    example = pd.DataFrame([
        {
            "claim_id": "CLM_000001",
            "policy_id": "POL_000001",
            "customer_id": "CUST_000001",
            "claim_amount": 12000.0,
            "severity_level": "Medium",
            "claim_status": "Open",
            "witness_count": 0,
            "photos_taken": 2,
            "days_to_report": 5,
            "premium_coverage_ratio": 0.01,
            "state": "CA",
            "fraud_score": 20.0,
        }
    ])

    for agent_name in AGENTS:
        uc_model_name = f"{UC_CATALOG}.{UC_SCHEMA}.{MODEL_PREFIX}_{agent_name}"
        print(f"Registering model: {uc_model_name}")

        agent = build_agent_model(agent_name)
        pyfunc = AgentPyFuncModel(agent)
        signature = infer_signature(example, pyfunc.predict(None, example))

        with mlflow.start_run(run_name=f"register_{agent_name}"):
            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=pyfunc,
                signature=signature,
                input_example=example,
            )
            run_id = mlflow.active_run().info.run_id

        mv = mlflow.register_model(
            model_uri=f"runs:/{run_id}/model",
            name=uc_model_name,
        )
        client.set_registered_model_alias(uc_model_name, "champion", mv.version)


def create_serving_endpoints():
    import requests
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    assert host and token, "Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    for agent_name in AGENTS:
        uc_model_name = f"{UC_CATALOG}.{UC_SCHEMA}.{MODEL_PREFIX}_{agent_name}"
        endpoint_name = f"{MODEL_PREFIX}-{agent_name}-endpoint"
        payload = {
            "name": endpoint_name,
            "config": {
                "served_models": [
                    {
                        "name": f"{agent_name}-champion",
                        "model_name": uc_model_name,
                        "model_version": "aliases/champion",
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    }
                ]
            }
        }
        url = f"{host}/api/2.0/serving-endpoints"
        r = requests.post(url, headers=headers, data=json.dumps(payload))
        if r.status_code == 409:
            # Update existing endpoint
            print(f"Updating endpoint: {endpoint_name}")
            url_upd = f"{host}/api/2.0/serving-endpoints/{endpoint_name}/config"
            r = requests.put(url_upd, headers=headers, data=json.dumps(payload["config"]))
        r.raise_for_status()
        print(f"Provisioned endpoint: {endpoint_name}")


if __name__ == "__main__":
    register_models()
    create_serving_endpoints()
