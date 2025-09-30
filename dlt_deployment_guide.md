# Delta Live Tables Pipeline Deployment Guide

## Overview
This guide explains how to deploy the Delta Live Tables (DLT) pipeline for ingesting content insurance data into `shivam_catalog.shivam_schema`.

## Prerequisites
1. Databricks workspace with DLT enabled
2. Access to `shivam_catalog` and `shivam_schema`
3. Generated parquet files in `/FileStore/shared_uploads/insurance_data/`

## Files Created
- `dlt_pipeline.py` - Main DLT pipeline code
- `dlt_pipeline_config.json` - Pipeline configuration
- `dlt_deployment_guide.md` - This deployment guide

## Deployment Steps

### 1. Upload Files to Databricks
1. Upload `dlt_pipeline.py` to your Databricks workspace
2. Place it in `/Workspace/Repos/shivampanicker/agentbricks/` or your preferred location

### 2. Create DLT Pipeline via UI
1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure the pipeline:
   - **Pipeline name**: `content_insurance_dlt_pipeline`
   - **Source type**: Notebook
   - **Notebook path**: `/Workspace/Repos/shivampanicker/agentbricks/dlt_pipeline.py`
   - **Target schema**: `shivam_catalog.shivam_schema`
   - **Storage location**: `/FileStore/shared_uploads/dlt_storage/`

### 3. Configure Pipeline Settings
- **Edition**: Advanced
- **Photon**: Enabled
- **Channel**: Current
- **Development mode**: Enabled (for testing)
- **Continuous**: Disabled (triggered runs)

### 4. Cluster Configuration
- **Node type**: i3.xlarge
- **Driver node**: i3.xlarge
- **Workers**: 2 (auto-scale 1-4)
- **Photon**: Enabled

### 5. Run the Pipeline
1. Click **Start** to run the pipeline
2. Monitor the execution in the pipeline UI
3. Check data quality metrics in the **Data Quality** tab

## Data Quality Constraints

### Policies Table (10 constraints)
- Valid policy IDs and customer IDs
- Coverage amount: 0 < amount ≤ 10M
- Premium amount: 0 < amount ≤ 100K
- Deductible: 0 ≤ amount ≤ 10K
- Date validation: start ≤ end
- Risk score: 200-850
- Discount: 0-50%
- Square footage: 0 < sqft ≤ 10K
- Construction year: 1900-2024
- Premium/coverage ratio: 0.001-0.05

### Customers Table (12 constraints)
- Valid customer IDs and names
- Email format validation
- Phone number validation
- ZIP code: 5 digits
- State: 2 characters
- Country: USA only
- Years at address: 0-50
- Previous claims: 0-20
- Age group format validation
- Income range format validation

### Claims Table (18 constraints)
- Valid claim, policy, and customer IDs
- Claim amount: 0 < amount ≤ 10M
- Settlement amount: 0 ≤ amount ≤ claim amount
- Deductible: 0 ≤ amount ≤ 10K
- Fraud score: 0-100
- Witness count: 0-50
- Photos taken: 0-100
- Repair estimate: 0 ≤ amount ≤ 20M
- Date validation: incident ≤ claim ≤ report
- Settlement ratio: 0-1.1
- Valid severity levels
- Valid claim types and statuses
- Comments required (expect, not drop)

## Tables Created

### Core Tables
- `shivam_catalog.shivam_schema.policies`
- `shivam_catalog.shivam_schema.customers`
- `shivam_catalog.shivam_schema.claims`

### Enriched Views
- `shivam_catalog.shivam_schema.enriched_claims`

### Quality & Audit Tables
- `shivam_catalog.shivam_schema.data_quality_summary`
- `shivam_catalog.shivam_schema.data_lineage_audit`

## Monitoring and Maintenance

### Data Quality Monitoring
1. Check the **Data Quality** tab in the pipeline UI
2. Review failed records and adjust constraints if needed
3. Monitor data quality scores in `data_quality_summary` table

### Performance Optimization
1. Enable auto-optimize for managed tables
2. Monitor cluster utilization
3. Adjust cluster size based on data volume

### Troubleshooting
1. Check pipeline logs for errors
2. Verify source data paths are correct
3. Ensure catalog and schema permissions
4. Review data quality constraint violations

## Next Steps
1. Run the pipeline in development mode first
2. Validate data quality and business rules
3. Switch to production mode for continuous processing
4. Set up monitoring and alerting
5. Create downstream analytics and reporting
