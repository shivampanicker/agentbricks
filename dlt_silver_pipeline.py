# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables Silver Pipeline for Content Insurance Data
# MAGIC 
# MAGIC This pipeline reads Bronze layer tables and standardizes them into conformed dimensions and a fact table in `suncorp_catalog.suncorp_silver_schema`.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG_NAME = "suncorp_catalog"
SILVER_SCHEMA = "suncorp_silver_schema"

# Configure target for DLT (set in pipeline UI as target: suncorp_silver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

@dlt.table(
    name="dim_customer",
    comment="Conformed customer dimension",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    "pk_not_null": "customer_sk IS NOT NULL",
    "customer_id_present": "customer_id IS NOT NULL AND customer_id != ''",
    "email_valid": "email LIKE '%@%.%'",
    "state_len": "LENGTH(state) = 2"
})
def dim_customer():
    src = dlt.read("customers")
    return (
        src
        .select(
            F.upper(F.col("customer_id")).alias("customer_id"),
            F.initcap(F.col("first_name")).alias("first_name"),
            F.initcap(F.col("last_name")).alias("last_name"),
            F.lower(F.col("email")).alias("email"),
            F.regexp_replace(F.col("phone"), "[^0-9]", "").alias("phone_e164_like"),
            F.to_date("date_of_birth").alias("date_of_birth"),
            F.col("age_group"),
            F.initcap("city").alias("city"),
            F.upper("state").alias("state"),
            F.col("zip_code").alias("postal_code"),
            F.upper("country").alias("country"),
            F.initcap("occupation").alias("occupation"),
            F.col("previous_claims_count").alias("lifetime_claims_count"),
            F.to_date("customer_since").alias("customer_since"),
            F.to_date("last_contact_date").alias("last_contact_date"),
            F.sha2(F.concat_ws("|", F.col("customer_id")), 256).alias("customer_sk")
        )
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

@dlt.table(
    name="dim_policy",
    comment="Conformed policy dimension",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    "pk_not_null": "policy_sk IS NOT NULL",
    "policy_id_present": "policy_id IS NOT NULL AND policy_id != ''",
    "coverage_positive": "coverage_amount > 0",
    "premium_positive": "premium_amount > 0"
})
def dim_policy():
    src = dlt.read("policies")
    return (
        src
        .select(
            F.upper("policy_id").alias("policy_id"),
            F.upper("customer_id").alias("customer_id"),
            F.initcap("policy_type").alias("policy_type"),
            F.col("coverage_amount"),
            F.col("premium_amount"),
            F.col("deductible"),
            F.initcap("property_type").alias("property_type"),
            F.col("construction_year"),
            F.col("square_footage"),
            F.initcap("policy_status").alias("policy_status"),
            F.upper("region").alias("region"),
            F.col("zip_code").alias("postal_code"),
            F.to_date("policy_start_date").alias("policy_start_date"),
            F.to_date("policy_end_date").alias("policy_end_date"),
            (F.col("premium_amount")/F.col("coverage_amount")).alias("premium_coverage_ratio"),
            F.sha2(F.concat_ws("|", F.col("policy_id")), 256).alias("policy_sk")
        )
        .dropDuplicates(["policy_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact

# COMMAND ----------

@dlt.table(
    name="fact_claim",
    comment="Claims fact table with conformed dimensions and metrics",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    "fk_policy": "policy_id IS NOT NULL AND policy_id != ''",
    "fk_customer": "customer_id IS NOT NULL AND customer_id != ''",
    "amount_positive": "claim_amount > 0",
    "deductible_nonneg": "deductible_applied >= 0",
    "severity_valid": "severity_level IN ('Low','Medium','High','Critical')"
})
@dlt.expect_or_drop({
    "settlement_ratio_valid": "settlement_amount/claim_amount BETWEEN 0 AND 1.1"
})
def fact_claim():
    claims = dlt.read("claims")
    dim_cust = dlt.read("dim_customer")
    dim_pol = dlt.read("dim_policy")

    joined = (
        claims.alias("c")
        .join(dim_pol.select("policy_id", "policy_sk", "customer_id").alias("p"), ["policy_id"], "left")
        .join(dim_cust.select("customer_id", "customer_sk").alias("d"), ["customer_id"], "left")
    )

    return (
        joined
        .select(
            F.upper(F.col("c.claim_id")).alias("claim_id"),
            F.col("p.policy_sk"),
            F.col("d.customer_sk"),
            F.col("c.policy_id"),
            F.col("c.customer_id"),
            F.initcap("c.claim_type").alias("claim_type"),
            F.initcap("c.claim_status").alias("claim_status"),
            F.initcap("c.severity_level").alias("severity_level"),
            F.to_date("c.claim_date").alias("claim_date"),
            F.to_date("c.incident_date").alias("incident_date"),
            F.to_date("c.report_date").alias("report_date"),
            F.col("c.claim_amount"),
            F.col("c.settlement_amount"),
            F.col("c.deductible_applied"),
            (F.col("c.settlement_amount") - F.col("c.deductible_applied")).alias("net_settlement_amount"),
            (F.col("c.settlement_amount")/F.col("c.claim_amount")).alias("settlement_ratio"),
            F.col("c.fraud_score"),
            F.col("c.witness_count"),
            F.col("c.photos_taken"),
            F.col("c.repair_estimate"),
            F.length(F.col("c.comments")).alias("comment_length"),
            F.col("c.comments").alias("claim_comment"),
            F.current_timestamp().alias("etl_loaded_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conformance Views

# COMMAND ----------

@dlt.view(name="claim_daily_summary", comment="Daily aggregated claim metrics")
def claim_daily_summary():
    f = dlt.read("fact_claim")
    return (
        f.groupBy("claim_date", "claim_type", "severity_level")
         .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.sum("settlement_amount").alias("total_settlement"),
            F.avg("settlement_ratio").alias("avg_settlement_ratio")
         )
    )
