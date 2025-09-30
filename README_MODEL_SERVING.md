# AgentBricks Claims Agents - Model Serving

## Overview
This package contains four claims-processing agents (triage, fraud, settlement, routing) wrapped as MLflow pyfunc models and deployed to Databricks Model Serving.

## Prerequisites
- Databricks workspace with Unity Catalog and Model Serving enabled
- Permissions on target catalog/schema
- Environment variables:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`
  - Optional: `UC_CATALOG` (default `suncorp_catalog`), `UC_SCHEMA` (default `suncorp_models_schema`), `MODEL_PREFIX` (default `agentbricks_claims`)

## Register Models and Create Endpoints
```bash
# In a Databricks notebook or local with Databricks-connect enabled
pip install mlflow requests
python register_and_serve_agents.py
```

This will:
- Register four models in UC: `{UC_CATALOG}.{UC_SCHEMA}.{MODEL_PREFIX}_{agent}`
- Create/Update four serving endpoints: `{MODEL_PREFIX}-{agent}-endpoint`

## Invoke Endpoints
```bash
curl -X POST \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  "$DATABRICKS_HOST/api/2.0/serving-endpoints/agentbricks-claims-triage-endpoint/invocations" \
  -d '{
    "dataframe_records": [{
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
      "fraud_score": 20.0
    }] 
  }'
```

## Notes
- The agents are currently rule-based; plug in an LLM via `LLMClient` if desired.
- Models use MLflow pyfunc for simple tabular IO. Adjust signature as needed for production.
