# renewal-plan Edge Function

## Deploy

```bash
supabase functions deploy renewal-plan
```

## Required secret

`INGEST_SECRET` must be set in your Supabase project secrets.

## Test with curl

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/renewal-plan" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{}'
```
