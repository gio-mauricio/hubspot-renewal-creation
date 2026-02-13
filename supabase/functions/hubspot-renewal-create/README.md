# hubspot-renewal-create Edge Function

Creates forecast renewal deals from `public.renewals_ready_for_hubspot`.

## Deploy

```bash
supabase functions deploy hubspot-renewal-create
```

## Required secrets

- `INGEST_SECRET`
- `HUBSPOT_PRIVATE_APP_TOKEN`
- `RUN_MODE` (must be `test`)

Optional:

- `FORECAST_PIPELINE_ID`
- `FORECAST_DEALSTAGE_ID`

## Trigger example

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/hubspot-renewal-create" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{"limit":5,"create_line_items":true}'
```

## Manual single-deal trigger

Use this to create immediately for one source deal that is already ready:

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/hubspot-renewal-create" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{
    "source_hubspot_deal_id": "30135783473",
    "create_line_items": true
  }'
```
