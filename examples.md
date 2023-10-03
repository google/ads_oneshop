## List resources in subordinate accounts

```bash
python -m acit.resource_downloader --api_name='content' --api_version='v2.1' --resource_name='products' --params='merchantId=@id' --parent_resource='accounts' --parent_params='merchantId=$MERCHANT_ID'
```

## Run a query

```bash
export QUERY='SELECT
  segments.title,
  segments.offer_id,
  metrics.impressions,
  metrics.clicks
FROM MerchantPerformanceView
WHERE segments.date DURING LAST_30_DAYS
ORDER BY metrics.clicks DESC'

python -m acit.resource_downloader --api_name='content' --api_version='v2.1' --resource_name='reports' --resource_method='search' --params="merchantId=$MERCHANT_ID;body={\"query\": \"$(echo $QUERY)\"}" --result_path='results'
```
