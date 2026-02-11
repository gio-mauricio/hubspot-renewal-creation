/// <reference types="https://esm.sh/@supabase/functions-js/src/edge-runtime.d.ts" />

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

type YouniumSubscription = {
  id?: string;
  status?: string;
  accountNumber?: string | null;
  account?: unknown;
  effectiveStartDate?: string | null;
  effectiveEndDate?: string | null;
  cancellationDate?: string | null;
  [key: string]: unknown;
};

type TokenResponse = {
  access_token?: string;
  accessToken?: string;
};

type RuntimeConfig = {
  youniumBaseUrl: string;
  youniumClientId: string;
  youniumSecret: string;
  youniumLegalEntity: string;
  youniumApiVersion: string;
  pageSize: number;
  supabaseUrl: string;
  supabaseServiceRoleKey: string;
  ingestSecret: string;
};

function jsonResponse(status: number, payload: Record<string, unknown>): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'Content-Type': 'application/json' }
  });
}

function getRequiredEnv(name: string): string {
  const value = Deno.env.get(name)?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getPageSize(raw: string | undefined): number {
  const fallback = 200;
  if (!raw || raw.trim() === '') {
    return fallback;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid PAGE_SIZE: ${raw}`);
  }

  return parsed;
}

function loadConfig(): RuntimeConfig {
  return {
    youniumBaseUrl: getRequiredEnv('YOUNIUM_BASE_URL').replace(/\/$/, ''),
    youniumClientId: getRequiredEnv('YOUNIUM_CLIENT_ID'),
    youniumSecret: getRequiredEnv('YOUNIUM_SECRET'),
    youniumLegalEntity: getRequiredEnv('YOUNIUM_LEGAL_ENTITY'),
    youniumApiVersion: Deno.env.get('YOUNIUM_API_VERSION')?.trim() || '2.1',
    pageSize: getPageSize(Deno.env.get('PAGE_SIZE')),
    supabaseUrl: getRequiredEnv('SUPABASE_URL'),
    supabaseServiceRoleKey: getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY'),
    ingestSecret: getRequiredEnv('INGEST_SECRET')
  };
}

function isAuthorized(request: Request, expectedSecret: string): boolean {
  const secret = request.headers.get('x-ingest-secret');
  if (!secret) {
    return false;
  }

  return secret === expectedSecret;
}

function parseSubscriptionsPayload(payload: unknown): YouniumSubscription[] {
  if (Array.isArray(payload)) {
    return payload as YouniumSubscription[];
  }

  if (payload && typeof payload === 'object') {
    const record = payload as Record<string, unknown>;

    if (Array.isArray(record.items)) {
      return record.items as YouniumSubscription[];
    }

    if (Array.isArray(record.data)) {
      return record.data as YouniumSubscription[];
    }

    if (Array.isArray(record.value)) {
      return record.value as YouniumSubscription[];
    }
  }

  return [];
}

async function getAccessToken(config: RuntimeConfig): Promise<string> {
  const response = await fetch(`${config.youniumBaseUrl}/auth/v2/token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify({
      clientId: config.youniumClientId,
      secret: config.youniumSecret
    })
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`Token request failed (${response.status}): ${text}`);
  }

  let body: TokenResponse;
  try {
    body = JSON.parse(text) as TokenResponse;
  } catch {
    throw new Error(`Failed to parse token JSON: ${text}`);
  }

  const token = body.access_token ?? body.accessToken;
  if (!token) {
    throw new Error('Token response did not include access_token or accessToken');
  }

  return token;
}

async function fetchSubscriptionsPage(
  config: RuntimeConfig,
  accessToken: string,
  pageNumber: number
): Promise<YouniumSubscription[]> {
  const url = new URL(`${config.youniumBaseUrl}/Subscriptions`);
  url.searchParams.set('PageSize', String(config.pageSize));
  url.searchParams.set('PageNumber', String(pageNumber));

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: 'application/json',
      'api-version': config.youniumApiVersion,
      'legal-entity': config.youniumLegalEntity
    }
  });

  const text = await response.text();

  if (!response.ok) {
    const noMorePagesMessage = 'No subscriptions of latest version could be found';
    if (response.status === 400 && text.includes(noMorePagesMessage)) {
      return [];
    }

    throw new Error(`Subscriptions request failed (${response.status}) for page ${pageNumber}: ${text}`);
  }

  let payload: unknown;
  try {
    payload = JSON.parse(text);
  } catch {
    throw new Error(`Failed to parse subscriptions JSON for page ${pageNumber}: ${text}`);
  }

  return parseSubscriptionsPayload(payload);
}

function isActiveSubscription(subscription: YouniumSubscription): subscription is YouniumSubscription & { id: string; status: 'Active' } {
  return subscription.status === 'Active' && typeof subscription.id === 'string' && subscription.id.length > 0;
}

function asNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function resolveAccountNumber(subscription: YouniumSubscription): string | null {
  const direct = asNonEmptyString(subscription.accountNumber);
  if (direct) {
    return direct;
  }

  const account = subscription.account;

  if (Array.isArray(account)) {
    for (const item of account) {
      if (!item || typeof item !== 'object') {
        continue;
      }

      const nested = asNonEmptyString((item as Record<string, unknown>).accountNumber);
      if (nested) {
        return nested;
      }
    }

    return null;
  }

  if (account && typeof account === 'object') {
    return asNonEmptyString((account as Record<string, unknown>).accountNumber);
  }

  return null;
}

Deno.serve(async (request: Request) => {
  if (request.method !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed. Use POST.' });
  }

  let config: RuntimeConfig;
  try {
    config = loadConfig();
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, { error: message });
  }

  if (!isAuthorized(request, config.ingestSecret)) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  try {
    const supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
      auth: { persistSession: false }
    });

    const accessToken = await getAccessToken(config);

    let pageNumber = 1;
    let pages = 0;
    let fetchedTotal = 0;
    let activeTotal = 0;
    let upsertedTotal = 0;

    while (true) {
      const subscriptions = await fetchSubscriptionsPage(config, accessToken, pageNumber);

      if (subscriptions.length === 0) {
        break;
      }

      pages += 1;
      fetchedTotal += subscriptions.length;

      const activeSubscriptions = subscriptions.filter(isActiveSubscription);
      activeTotal += activeSubscriptions.length;

      if (activeSubscriptions.length > 0) {
        const nowIso = new Date().toISOString();
        const rows = activeSubscriptions.map((subscription) => ({
          subscription_id: subscription.id,
          account_number: resolveAccountNumber(subscription),
          status: subscription.status,
          effective_start_date: subscription.effectiveStartDate ?? null,
          effective_end_date: subscription.effectiveEndDate ?? null,
          cancellation_date: subscription.cancellationDate ?? null,
          raw_json: subscription,
          updated_at: nowIso
        }));

        const { error } = await supabase
          .from('younium_subscriptions')
          .upsert(rows, { onConflict: 'subscription_id' });

        if (error) {
          throw new Error(`Supabase upsert failed on page ${pageNumber}: ${error.message}`);
        }

        upsertedTotal += rows.length;
      }

      pageNumber += 1;
    }

    return jsonResponse(200, {
      pages,
      fetched_total: fetchedTotal,
      active_total: activeTotal,
      upserted_total: upsertedTotal
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, { error: message });
  }
});
