# hubspot-renewal-enqueue

Manually queue a renewal from a HubSpot deal (for early/off-cycle renewals) with dedupe safeguards.

## Deploy

```bash
supabase functions deploy hubspot-renewal-enqueue
```

## Required secrets

`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are available at runtime.

Set these secrets:

```bash
supabase secrets set \
  INGEST_SECRET="..." \
  HUBSPOT_PRIVATE_APP_TOKEN="..."
```

Optional:

```bash
supabase secrets set HUBSPOT_COMPANY_YOUNIUM_CUSTID_PROP="younium_custid"
```

Optional (only if you want to override default internal function URL):

```bash
supabase secrets set SUPABASE_FUNCTIONS_BASE_URL="https://<project_ref>.supabase.co/functions/v1"
```

## Invoke (curl)

```bash
curl -i -X POST "https://<project_ref>.supabase.co/functions/v1/hubspot-renewal-enqueue" \
  -H "x-ingest-secret: <INGEST_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{
    "source_hubspot_deal_id": "30135783473"
  }'
```

## What it does

1. Validates `x-ingest-secret`.
2. Fetches source deal from HubSpot.
3. Fetches associated company and reads `younium_custid` (or configured property).
4. Finds matching subscription(s) in `public.younium_subscriptions` by `account_number`.
5. Picks the best subscription based on source-deal date context.
6. Inserts/updates `public.renewal_ledger` with dedupe behavior:
   - `queued_new`
   - `already_queued`
   - `already_created`
   - `requeued_from_error`
7. Immediately triggers:
   - `renewal-snapshot` (filtered by source deal id)
   - `hubspot-renewal-create` (filtered by source deal id, with line items)

## Notes

- This endpoint is `POST` only.
- It never creates duplicate ledger rows because ledger identity is `(subscription_id, term_end_date)`.
- Business outcomes return HTTP `200` with a clear `result`, `message`, and `immediate_run` details.
