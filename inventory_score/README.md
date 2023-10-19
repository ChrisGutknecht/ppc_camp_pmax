# Prerequisites for calculating inventory score

- Activate the daily Google merchant transfer in the BigQuery data transfer. This is a free service, you only need a Google cloud account with a billing source and a Merchant Center account
- In the SQL code, the first CTE or common table expression includes a <code>from</code> statement with a dynamic reference <code>{{ ref('stg_gmc_product_attributes_daily') }}</code>. If you're not using a transformation framework like Dataform or dbt, just paste the direct table reference in there.
- You need sufficient historical data to run the historic best selection lookback logic. Alternatively, you can reference you own product history table with stock states per SKU. 
- The main output of the model will be a column called <code>weighted_inventory_score</code>, which will have the same value for each SKU in a parent group.
- Note that this implementation uses an aggregation per color via the <code>parent_and_color</code> group attribute. If you want to change this to full parent selection, update the aggregation to your needs. 