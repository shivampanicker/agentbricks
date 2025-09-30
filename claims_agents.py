# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Processing Agents (Silver Layer)
# MAGIC 
# MAGIC Agents:
# MAGIC - Triage Agent: priority scoring and SLA classification
# MAGIC - Fraud Agent: rule-based fraud risk scoring
# MAGIC - Settlement Agent: estimate reserves and suggested settlement
# MAGIC - Routing Agent: assign adjuster and recommended next action

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "suncorp_catalog"
SCHEMA = "suncorp_silver_schema"

FACT_TABLE = f"{CATALOG}.{SCHEMA}.fact_claim"
DIM_POLICY = f"{CATALOG}.{SCHEMA}.dim_policy"
DIM_CUSTOMER = f"{CATALOG}.{SCHEMA}.dim_customer"

# Output tables
TRIAGE_TABLE = f"{CATALOG}.{SCHEMA}.agent_triage"
FRAUD_TABLE = f"{CATALOG}.{SCHEMA}.agent_fraud"
SETTLEMENT_TABLE = f"{CATALOG}.{SCHEMA}.agent_settlement"
ROUTING_TABLE = f"{CATALOG}.{SCHEMA}.agent_routing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load inputs

# COMMAND ----------

claims = spark.table(FACT_TABLE)
policies = spark.table(DIM_POLICY)
customers = spark.table(DIM_CUSTOMER)

base = (
    claims.alias("c")
    .join(policies.select("policy_id", "policy_status", "premium_coverage_ratio").alias("p"), ["policy_id"], "left")
    .join(customers.select("customer_id", "lifetime_claims_count", "state").alias("d"), ["customer_id"], "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Triage Agent

# COMMAND ----------

triage = (
    base
    .withColumn("priority_score",
        F.col("claim_amount")/1000 +
        F.when(F.col("severity_level") == "Critical", 30)
         .when(F.col("severity_level") == "High", 20)
         .when(F.col("severity_level") == "Medium", 10)
         .otherwise(5) +
        F.when(F.col("claim_status").isin("Open", "In Progress"), 5).otherwise(0) +
        F.when(F.col("days_between(c.incident_date,c.claim_date)") > 30, -5).otherwise(0)
    )
    .withColumn("priority_band",
        F.when(F.col("priority_score") >= 60, "P1")
         .when(F.col("priority_score") >= 40, "P2")
         .when(F.col("priority_score") >= 20, "P3")
         .otherwise("P4")
    )
    .select(
        "claim_id", "policy_id", "customer_id", "claim_type", "claim_status", "severity_level",
        "claim_date", "incident_date", "claim_amount",
        "priority_score", "priority_band"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Agent

# COMMAND ----------

fraud = (
    base
    .withColumn("recent_claims_flag", F.when(F.col("lifetime_claims_count") >= 3, 10).otherwise(0))
    .withColumn("high_amount_flag", F.when(F.col("claim_amount") >= 50000, 15).otherwise(0))
    .withColumn("low_evidence_flag", F.when((F.col("witness_count") == 0) & (F.col("photos_taken") < 3), 10).otherwise(0))
    .withColumn("late_report_flag", F.when(F.datediff(F.col("report_date"), F.col("incident_date")) > 14, 8).otherwise(0))
    .withColumn("policy_ratio_flag", F.when(F.col("premium_coverage_ratio") < 0.005, 5).otherwise(0))
    .withColumn("state_risk_flag", F.when(F.col("state").isin("FL", "LA", "TX"), 5).otherwise(0))
    .withColumn("fraud_risk_score",
        F.col("recent_claims_flag") + F.col("high_amount_flag") + F.col("low_evidence_flag") +
        F.col("late_report_flag") + F.col("policy_ratio_flag") + F.col("state_risk_flag")
    )
    .withColumn("fraud_risk_band",
        F.when(F.col("fraud_risk_score") >= 35, "High")
         .when(F.col("fraud_risk_score") >= 20, "Medium")
         .otherwise("Low")
    )
    .select(
        "claim_id", "policy_id", "customer_id", "claim_type", "claim_status",
        "fraud_risk_score", "fraud_risk_band"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Settlement Agent

# COMMAND ----------

settlement = (
    base
    .withColumn("base_reserve", F.col("claim_amount") * F.when(F.col("severity_level") == "Critical", 0.9)
                                              .when(F.col("severity_level") == "High", 0.7)
                                              .when(F.col("severity_level") == "Medium", 0.5)
                                              .otherwise(0.3))
    .withColumn("fraud_adjustment", F.when(F.col("fraud_score") >= 70, -0.1)
                                      .when(F.col("fraud_score") >= 40, -0.05)
                                      .otherwise(0.0))
    .withColumn("reserve_amount", (F.col("base_reserve") * (1 + F.col("fraud_adjustment"))).cast("double"))
    .withColumn("suggested_settlement", (F.col("reserve_amount") * 0.85).cast("double"))
    .select(
        "claim_id", "policy_id", "customer_id", "claim_type", "severity_level",
        "claim_amount", "reserve_amount", "suggested_settlement"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Routing Agent

# COMMAND ----------

routing = (
    base
    .withColumn("channel", F.when(F.col("claim_amount") < 2000, "Straight-Through Processing")
                             .when(F.col("severity_level").isin("Low", "Medium"), "Adjuster")
                             .otherwise("Senior Adjuster"))
    .withColumn("assigned_adjuster",
        F.when(F.col("channel") == "Straight-Through Processing", F.lit("AUTO"))
         .when(F.col("channel") == "Adjuster", F.concat(F.lit("ADJ_"), F.lpad((F.rand()*50).cast("int"), 3, "0")))
         .otherwise(F.concat(F.lit("SADJ_"), F.lpad((F.rand()*20).cast("int"), 3, "0")))
    )
    .withColumn("recommended_action",
        F.when(F.col("channel") == "Straight-Through Processing", "Auto-approve if checks pass")
         .when(F.col("severity_level") == "Critical", "Escalate to senior adjuster")
         .when(F.col("severity_level") == "High", "Expedite investigation")
         .otherwise("Standard processing")
    )
    .select(
        "claim_id", "policy_id", "customer_id", "claim_type", "claim_status",
        "channel", "assigned_adjuster", "recommended_action"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Outputs

# COMMAND ----------

(
    triage.write.mode("overwrite").format("delta").saveAsTable(TRIAGE_TABLE)
)
(
    fraud.write.mode("overwrite").format("delta").saveAsTable(FRAUD_TABLE)
)
(
    settlement.write.mode("overwrite").format("delta").saveAsTable(SETTLEMENT_TABLE)
)
(
    routing.write.mode("overwrite").format("delta").saveAsTable(ROUTING_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity Checks

# COMMAND ----------

print("Rows (triage, fraud, settlement, routing):",
      spark.table(TRIAGE_TABLE).count(),
      spark.table(FRAUD_TABLE).count(),
      spark.table(SETTLEMENT_TABLE).count(),
      spark.table(ROUTING_TABLE).count())
