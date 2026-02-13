# renewal-snapshot Edge Function

## Deploy

```bash
supabase functions deploy renewal-snapshot
```

## Trigger via curl

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/renewal-snapshot" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Manual single-deal snapshot

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/renewal-snapshot" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{
    "source_hubspot_deal_id": "30135783473"
  }'
```
