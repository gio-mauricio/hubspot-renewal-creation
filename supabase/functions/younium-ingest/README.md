# younium-ingest Edge Function

## Deploy

```bash
supabase functions deploy younium-ingest
```

## Set secrets

```bash
supabase secrets set \
  YOUNIUM_BASE_URL="https://api.younium.com" \
  YOUNIUM_CLIENT_ID="..." \
  YOUNIUM_SECRET="..." \
  YOUNIUM_LEGAL_ENTITY="..." \
  YOUNIUM_API_VERSION="2.1" \
  PAGE_SIZE="200" \
  SUPABASE_URL="https://<project-ref>.supabase.co" \
  SUPABASE_SERVICE_ROLE_KEY="..." \
  INGEST_SECRET="<shared-secret>"
```

## Trigger example

```bash
curl -X POST "https://<project-ref>.functions.supabase.co/younium-ingest" \
  -H "Authorization: Bearer <shared-secret>" \
  -H "Content-Type: application/json"
```

Expected response shape:

```json
{
  "pages": 0,
  "fetched_total": 0,
  "active_total": 0,
  "upserted_total": 0
}
```
