# hubspot-renewal-create Edge Function

Creates up to 5 forecast renewal deals per invocation for HubSpot SANDBOX testing.

## Deploy

```bash
supabase functions deploy hubspot-renewal-create
```

## Required secrets

- `INGEST_SECRET`
- `HUBSPOT_PRIVATE_APP_TOKEN_SANDBOX`

Optional:

- `FORECAST_PIPELINE_ID`
- `FORECAST_DEALSTAGE_ID`

## Trigger example

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/hubspot-renewal-create" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{"limit":5}'
```
