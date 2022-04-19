## Overview
This directory contains a set of config files for modeling salesforce data. It also includes several common metrics calculated from Salesforce leads and opportunities. If you see a metric that's missing please open an issue.

The config files are based on tables loaded into your data warehouse using Fivetran and use the dimensions, identifier and measures names from these tables. If you are using a different ETL tool, you may need to update the dimensions, identifiers or measures, but the structure of the table should be the same.

## Steps to test

1. Add these configs to your MetricFlow model directory (this should be the same directory you specified in `model_path` during setup)
2. Update the `sql_table` parameter to point to your Salesforce tables. i.e salesforce.opportunity --> my_salesforce_schmea.my_opportunity_table
3. Run `mf validate-configs` to run validation on the configs. 
4. Test some of the metrics included in the configs by running the following commands
    * `mf query --metrics opps_created --dimensions ds`
    * `mf query --metrics closed_won_opps --dimensions ds --dimensions owner_id__full_name`
    * `mf query --metrics converted_leads --dimension ds --dimensions lead_source`
