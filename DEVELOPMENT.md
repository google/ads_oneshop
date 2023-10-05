# ACIT Development

## Developer environment setup

```bash
export PROJECT_ID="Your Project ID"
export MERCHANT_ID="A single Merchant ID you'd like to test"
export CUSTOMER_ID="A single Customer ID you'd like to test"
export LOGIN_CUSTOMER_ID="Required for access"

make && make test
gcloud config set project $PROJECT_ID
gcloud auth application-default login \
  --scopes="https://www.googleapis.com/auth/content"
gcloud auth application-default set-quota-project $PROJECT_ID

```

## Running ACIT Itself

### Main ACIT data downloader

Assumes the presence of a ~/google-ads.yaml file (for now).

```bash
python -m acit.acit \
  --merchant_id="$MERCHANT_ID" \
  --customer_id="$CUSTOMER_ID" \
  --login_customer_id="$LOGIN_CUSTOMER_ID"
```

### The Base Tables Pipeline

```bash
python -m acit.create_base_tables > wide_product_table.jsonlines
```

## Utilities

### List resources in subordinate accounts

```bash
python -m acit.resource_downloader \
  --api_name='content' \
  --api_version='v2.1' \
  --resource_name='products' \
  --params='merchantId=@id' \
  --parent_resource='accounts' \
  --parent_params="merchantId=$MERCHANT_ID"
```

### Run a query

```bash
export QUERY='SELECT
  segments.title,
  segments.offer_id,
  metrics.impressions,
  metrics.clicks
FROM MerchantPerformanceView
WHERE segments.date DURING LAST_30_DAYS
ORDER BY metrics.clicks DESC'

python -m acit.resource_downloader \
  --api_name='content' \
  --api_version='v2.1' \
  --resource_name='reports' \
  --resource_method='search' \
  --params="merchantId=$MERCHANT_ID;body={\"query\": \"$(echo $QUERY)\"}" \
  --result_path='results'
```

## Niceties

- Code coverage (VSCode extension Coverage Gutters can work with `make test`)
- Test debugging (absltest is unittest-based, so VSCode can set up test targets under the "`.`" root project directory)
- CLI test filtering with `python ci/run_tests.py  -- -k some_text_name`
- PDB debugging on fail with `--pdb_post_mortem` on most executables
- Easy debugging with JSONlines files if you have the `jq` utility

For example:

```bash
jq -c 'select(.isPMaxTargeted)' wide_product_table.jsonlines \
  | head -n 1 \
  | jq

cat $(find /tmp/acit -type f | grep listing_filter) \
  | jq -c '.assetGroup.id' \
  | less
```