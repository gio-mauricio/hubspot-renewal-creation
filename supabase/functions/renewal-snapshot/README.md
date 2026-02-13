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
