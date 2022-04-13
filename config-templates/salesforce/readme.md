## Overview
This directory contains a set of config files for modeling salesforce data. It also includes several common metrics calcuated from salesforce leads and opportunities. If you see a metric that's missing please open an issue in this repo.

The config files are based on tables loaded into your data warehouse using fivetran and use the dimensions, identifier and measures names from these tables. If you are using a diffrent ETL tool, you may need to update the dimension, identifier or measurers, but the structure of the table should be the same.

## Steps to test

1. Add thes configs to your metricflow model directory (this shoud be the same directory you specified in model_path during setup)
2. Update the sql_table parameter to point to your salesforce tables. i.e salesforce.opportunity --> my_salesforce_schmea.my_opportunity_table
3. Run mf validate-configs to run validataion on the configs. 
4. Test some of the metrics included in the configs by running the following commands
    * `mf query --metrics opps_created --dimensions ds`
    * `mf query --metrics closed_won_opps --dimensions ds --dimensions owner_id__full_name`
    * `mf query --metrics converted_leads --dimension ds --dimensions lead_source`

