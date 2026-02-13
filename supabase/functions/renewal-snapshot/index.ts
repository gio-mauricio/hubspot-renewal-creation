/// <reference types="https://esm.sh/@supabase/functions-js/src/edge-runtime.d.ts" />

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import {
  safeFinishAutomationRun,
  safeInsertAutomationEvents,
  safeStartAutomationRun
} from '../_shared/opsLogger.ts';

type RuntimeConfig = {
  ingestSecret: string;
  supabaseUrl: string;
  supabaseServiceRoleKey: string;
  youniumBaseUrl: string;
  youniumClientId: string;
  youniumSecret: string;
  youniumLegalEntity: string;
  youniumApiVersion: string;
  pageSize: number;
  batchSize: number;
  maxBatches: number;
};

type LedgerRow = {
  subscription_id: string;
  term_end_date: string;
};

type SnapshotKeyRow = {
  subscription_id: string;
  term_end_date: string;
};

type YouniumSubscriptionRow = {
  subscription_id: string;
  raw_json: unknown;
};

type TokenResponse = {
  access_token?: string;
  accessToken?: string;
};

type ErrorSample = {
  subscription_id: string;
  term_end_date: string;
  message: string;
};

type RequestOptions = {
  sourceHubspotDealId: string | null;
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

function parsePositiveInt(raw: string | undefined, defaultValue: number, name: string): number {
  if (!raw || raw.trim() === '') {
    return defaultValue;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }

  return parsed;
}

function loadConfig(): RuntimeConfig {
  return {
    ingestSecret: getRequiredEnv('INGEST_SECRET'),
    supabaseUrl: getRequiredEnv('SUPABASE_URL'),
    supabaseServiceRoleKey: getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY'),
    youniumBaseUrl: getRequiredEnv('YOUNIUM_BASE_URL').replace(/\/$/, ''),
    youniumClientId: getRequiredEnv('YOUNIUM_CLIENT_ID'),
    youniumSecret: getRequiredEnv('YOUNIUM_SECRET'),
    youniumLegalEntity: getRequiredEnv('YOUNIUM_LEGAL_ENTITY'),
    youniumApiVersion: Deno.env.get('YOUNIUM_API_VERSION')?.trim() || '2.1',
    pageSize: parsePositiveInt(Deno.env.get('PAGE_SIZE'), 100, 'PAGE_SIZE'),
    batchSize: parsePositiveInt(Deno.env.get('BATCH_SIZE'), 50, 'BATCH_SIZE'),
    maxBatches: parsePositiveInt(Deno.env.get('MAX_BATCHES'), 10, 'MAX_BATCHES')
  };
}

function getIngestSecretFromHeaders(headers: Headers): string | null {
  return headers.get('x-ingest-secret') ?? headers.get('x_ingest_secret');
}

function isAuthorized(req: Request, expectedSecret: string): boolean {
  const providedSecret = getIngestSecretFromHeaders(req.headers);
  return Boolean(providedSecret && providedSecret === expectedSecret);
}

function keyOf(row: SnapshotKeyRow): string {
  return `${row.subscription_id}::${row.term_end_date}`;
}

function asNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function asDealIdString(value: unknown): string | null {
  const fromString = asNonEmptyString(value);
  if (fromString) {
    return fromString;
  }

  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    return String(Math.trunc(value));
  }

  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function pickObjectString(obj: Record<string, unknown>, key: string): string | null {
  return asNonEmptyString(obj[key]);
}

function extractOrderId(rawJson: unknown): string | null {
  if (!isRecord(rawJson)) {
    return null;
  }

  return pickObjectString(rawJson, 'id') ?? pickObjectString(rawJson, 'orderId');
}

function parseArrayPayload(payload: unknown): unknown[] {
  if (Array.isArray(payload)) {
    return payload;
  }

  if (payload && typeof payload === 'object') {
    const record = payload as Record<string, unknown>;

    if (Array.isArray(record.items)) {
      return record.items;
    }

    if (Array.isArray(record.data)) {
      return record.data;
    }

    if (Array.isArray(record.value)) {
      return record.value;
    }
  }

  return [];
}

async function getRequestOptions(req: Request): Promise<RequestOptions> {
  const text = await req.text();
  if (!text.trim()) {
    return { sourceHubspotDealId: null };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error('Invalid JSON body');
  }

  if (!isRecord(parsed)) {
    throw new Error('Invalid request body: expected JSON object');
  }

  const sourceHubspotDealId =
    asDealIdString(parsed.source_hubspot_deal_id) ??
    asDealIdString(parsed.sourceDealId) ??
    asDealIdString(parsed.deal_id) ??
    asDealIdString(parsed.dealId) ??
    asDealIdString(parsed.hs_object_id) ??
    null;

  return { sourceHubspotDealId };
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

  let payload: TokenResponse;
  try {
    payload = JSON.parse(text) as TokenResponse;
  } catch {
    throw new Error(`Failed to parse token response: ${text}`);
  }

  const token = payload.access_token ?? payload.accessToken;
  if (!token) {
    throw new Error('Token response did not include access_token or accessToken');
  }

  return token;
}

async function fetchChargesPage(
  config: RuntimeConfig,
  accessToken: string,
  orderId: string,
  pageNumber: number
): Promise<unknown[]> {
  const url = new URL(`${config.youniumBaseUrl}/Orders/${orderId}/charges`);
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
    if (response.status === 400) {
      const endOfPaginationMessages = [
        'No order product charge could be found',
        'No subscriptions of latest version could be found'
      ];

      let responseMessageValues: string[] = [];
      try {
        const parsed = JSON.parse(text) as Record<string, unknown>;
        const errors = parsed.errors;

        if (errors && typeof errors === 'object') {
          const message = (errors as Record<string, unknown>).message;

          if (Array.isArray(message)) {
            responseMessageValues = message.filter((value): value is string => typeof value === 'string');
          } else if (typeof message === 'string') {
            responseMessageValues = [message];
          }
        }
      } catch {
        responseMessageValues = [];
      }

      const isEndOfPagination = responseMessageValues.some((responseMessage) =>
        endOfPaginationMessages.some((knownMessage) => responseMessage.includes(knownMessage))
      );

      if (isEndOfPagination) {
        return [];
      }
    }

    throw new Error(`Charges request failed (${response.status}) for order ${orderId}, page ${pageNumber}: ${text}`);
  }

  let payload: unknown;
  try {
    payload = JSON.parse(text);
  } catch {
    throw new Error(`Failed to parse charges JSON for order ${orderId}, page ${pageNumber}: ${text}`);
  }

  return parseArrayPayload(payload);
}

async function fetchAllCharges(config: RuntimeConfig, accessToken: string, orderId: string): Promise<unknown[]> {
  const allCharges: unknown[] = [];
  let pageNumber = 1;

  while (true) {
    const charges = await fetchChargesPage(config, accessToken, orderId, pageNumber);

    if (charges.length === 0) {
      break;
    }

    allCharges.push(...charges);
    pageNumber += 1;
  }

  return allCharges;
}

async function fetchPlannedRowsWithoutSnapshot(
  supabase: ReturnType<typeof createClient>,
  batchSize: number,
  sourceHubspotDealId: string | null
): Promise<LedgerRow[]> {
  if (sourceHubspotDealId) {
    const { data: filteredRowsData, error: filteredError } = await supabase
      .from('renewal_ledger')
      .select('subscription_id,term_end_date')
      .eq('status', 'planned')
      .eq('source_hubspot_deal_id', sourceHubspotDealId)
      .order('term_end_date', { ascending: true })
      .order('subscription_id', { ascending: true })
      .limit(batchSize);

    if (filteredError) {
      throw new Error(`Failed to query renewal_ledger: ${filteredError.message}`);
    }

    const filteredRows = (filteredRowsData ?? []) as LedgerRow[];
    if (filteredRows.length === 0) {
      return [];
    }

    const subscriptionIds = Array.from(new Set(filteredRows.map((row) => row.subscription_id)));
    const existingSnapshotKeys = new Set<string>();

    if (subscriptionIds.length > 0) {
      const { data: snapshotsData, error: snapshotsError } = await supabase
        .from('renewal_snapshots')
        .select('subscription_id,term_end_date')
        .in('subscription_id', subscriptionIds);

      if (snapshotsError) {
        throw new Error(`Failed to query renewal_snapshots: ${snapshotsError.message}`);
      }

      const snapshots = (snapshotsData ?? []) as SnapshotKeyRow[];
      for (const snapshot of snapshots) {
        existingSnapshotKeys.add(keyOf(snapshot));
      }
    }

    return filteredRows.filter((row) => !existingSnapshotKeys.has(keyOf(row)));
  }

  const scanChunk = Math.max(batchSize * 3, batchSize);
  const selected: LedgerRow[] = [];
  const selectedKeys = new Set<string>();
  let offset = 0;

  while (selected.length < batchSize) {
    const { data: plannedRowsData, error: plannedError } = await supabase
      .from('renewal_ledger')
      .select('subscription_id,term_end_date')
      .eq('status', 'planned')
      .order('term_end_date', { ascending: true })
      .order('subscription_id', { ascending: true })
      .range(offset, offset + scanChunk - 1);

    if (plannedError) {
      throw new Error(`Failed to query renewal_ledger: ${plannedError.message}`);
    }

    const plannedRows = (plannedRowsData ?? []) as LedgerRow[];
    if (plannedRows.length === 0) {
      break;
    }

    const uniquePlanned = new Map<string, LedgerRow>();
    for (const row of plannedRows) {
      if (!row.subscription_id || !row.term_end_date) {
        continue;
      }
      uniquePlanned.set(keyOf(row), row);
    }

    const subscriptionIds = Array.from(new Set(Array.from(uniquePlanned.values()).map((r) => r.subscription_id)));
    const existingSnapshotKeys = new Set<string>();

    if (subscriptionIds.length > 0) {
      const { data: snapshotsData, error: snapshotsError } = await supabase
        .from('renewal_snapshots')
        .select('subscription_id,term_end_date')
        .in('subscription_id', subscriptionIds);

      if (snapshotsError) {
        throw new Error(`Failed to query renewal_snapshots: ${snapshotsError.message}`);
      }

      const snapshots = (snapshotsData ?? []) as SnapshotKeyRow[];
      for (const snapshot of snapshots) {
        existingSnapshotKeys.add(keyOf(snapshot));
      }
    }

    for (const row of uniquePlanned.values()) {
      const rowKey = keyOf(row);
      if (existingSnapshotKeys.has(rowKey) || selectedKeys.has(rowKey)) {
        continue;
      }

      selected.push(row);
      selectedKeys.add(rowKey);

      if (selected.length >= batchSize) {
        break;
      }
    }

    if (plannedRows.length < scanChunk) {
      break;
    }

    offset += scanChunk;
  }

  return selected;
}

Deno.serve(async (req: Request): Promise<Response> => {
  if (req.method !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed. Use POST.' });
  }

  let config: RuntimeConfig;
  try {
    config = loadConfig();
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, { error: message });
  }

  if (!isAuthorized(req, config.ingestSecret)) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  let requestOptions: RequestOptions;
  try {
    requestOptions = await getRequestOptions(req);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(400, { error: message });
  }

  let supabase: ReturnType<typeof createClient> | null = null;
  let runId: string | null = null;
  let batchesProcessed = 0;
  let processedRowsTotal = 0;
  let snapshotsUpsertedTotal = 0;
  let errorsTotal = 0;
  const errorSamples: ErrorSample[] = [];

  try {
    supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
      auth: { persistSession: false }
    });
    runId = await safeStartAutomationRun(supabase, {
      functionName: 'renewal-snapshot',
      triggerSource: requestOptions.sourceHubspotDealId ? 'webhook' : 'cron',
      sourceHubspotDealId: requestOptions.sourceHubspotDealId
    });

    const addError = (row: LedgerRow, message: string): void => {
      errorsTotal += 1;
      if (errorSamples.length < 10) {
        errorSamples.push({
          subscription_id: row.subscription_id,
          term_end_date: row.term_end_date,
          message
        });
      }
    };

    while (batchesProcessed < config.maxBatches) {
      const ledgerBatch = await fetchPlannedRowsWithoutSnapshot(
        supabase,
        config.batchSize,
        requestOptions.sourceHubspotDealId
      );
      const fetchedBatchCount = ledgerBatch.length;

      if (fetchedBatchCount === 0) {
        break;
      }

      batchesProcessed += 1;
      processedRowsTotal += fetchedBatchCount;

      const subscriptionIds = Array.from(new Set(ledgerBatch.map((row) => row.subscription_id)));
      const { data: subscriptionRowsData, error: subscriptionsError } = await supabase
        .from('younium_subscriptions')
        .select('subscription_id,raw_json')
        .in('subscription_id', subscriptionIds);

      if (subscriptionsError) {
        throw new Error(`Failed to query younium_subscriptions: ${subscriptionsError.message}`);
      }

      const subscriptionRows = (subscriptionRowsData ?? []) as YouniumSubscriptionRow[];
      const subscriptionById = new Map<string, YouniumSubscriptionRow>();
      for (const row of subscriptionRows) {
        subscriptionById.set(row.subscription_id, row);
      }

      const snapshotRows: Array<Record<string, unknown>> = [];

      for (const ledgerRow of ledgerBatch) {
        const subscriptionRow = subscriptionById.get(ledgerRow.subscription_id);
        if (!subscriptionRow) {
          addError(ledgerRow, 'Subscription not found in public.younium_subscriptions');
          continue;
        }

        const orderId = extractOrderId(subscriptionRow.raw_json);
        if (!orderId) {
          addError(ledgerRow, 'Missing raw_json.orderId');
          continue;
        }

        try {
          const accessToken = await getAccessToken(config);
          const charges = await fetchAllCharges(config, accessToken, orderId);
          const nowIso = new Date().toISOString();

          snapshotRows.push({
            subscription_id: ledgerRow.subscription_id,
            term_end_date: ledgerRow.term_end_date,
            younium_charges_json: charges,
            snapshot_at: nowIso,
            updated_at: nowIso
          });
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error);
          addError(ledgerRow, message);
        }
      }

      if (snapshotRows.length > 0) {
        const { data: upsertedRows, error: upsertError } = await supabase
          .from('renewal_snapshots')
          .upsert(snapshotRows, {
            onConflict: 'subscription_id,term_end_date'
          })
          .select('subscription_id,term_end_date');

        if (upsertError) {
          throw new Error(`Failed to upsert renewal_snapshots: ${upsertError.message}`);
        }

        snapshotsUpsertedTotal += (upsertedRows ?? []).length;
      }

      if (fetchedBatchCount < config.batchSize) {
        break;
      }

      if (requestOptions.sourceHubspotDealId) {
        break;
      }
    }

    await safeInsertAutomationEvents(supabase, [
      {
        runId,
        functionName: 'renewal-snapshot',
        eventType: 'snapshot_summary',
        status: errorsTotal > 0 ? 'partial' : 'success',
        sourceHubspotDealId: requestOptions.sourceHubspotDealId,
        detail: {
          batch_size: config.batchSize,
          max_batches: config.maxBatches,
          batches_processed: batchesProcessed,
          processed_rows: processedRowsTotal,
          snapshots_upserted: snapshotsUpsertedTotal,
          errors: errorsTotal,
          error_samples: errorSamples
        }
      }
    ]);
    await safeFinishAutomationRun(supabase, {
      runId,
      status: errorsTotal > 0 ? 'partial' : 'success',
      httpStatus: 200,
      processedCount: processedRowsTotal,
      createdCount: snapshotsUpsertedTotal,
      errorCount: errorsTotal,
      metadata: {
        batch_size: config.batchSize,
        max_batches: config.maxBatches,
        batches_processed: batchesProcessed,
        source_hubspot_deal_id: requestOptions.sourceHubspotDealId
      }
    });

    return jsonResponse(200, {
      batch_size: config.batchSize,
      max_batches: config.maxBatches,
      requested_source_hubspot_deal_id: requestOptions.sourceHubspotDealId,
      batches_processed: batchesProcessed,
      snapshots_upserted: snapshotsUpsertedTotal,
      errors: errorsTotal,
      error_samples: errorSamples,
      timestamp: new Date().toISOString()
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);

    if (supabase) {
      await safeInsertAutomationEvents(supabase, [
        {
          runId,
          functionName: 'renewal-snapshot',
          eventType: 'run_failed',
          status: 'error',
          sourceHubspotDealId: requestOptions.sourceHubspotDealId,
          detail: {
            error: message,
            batch_size: config.batchSize,
            max_batches: config.maxBatches,
            batches_processed: batchesProcessed,
            processed_rows: processedRowsTotal,
            snapshots_upserted: snapshotsUpsertedTotal,
            errors: errorsTotal,
            error_samples: errorSamples
          }
        }
      ]);
      await safeFinishAutomationRun(supabase, {
        runId,
        status: 'error',
        httpStatus: 500,
        processedCount: processedRowsTotal,
        createdCount: snapshotsUpsertedTotal,
        errorCount: Math.max(errorsTotal, 1),
        errorMessage: message,
        metadata: {
          batch_size: config.batchSize,
          max_batches: config.maxBatches,
          batches_processed: batchesProcessed,
          source_hubspot_deal_id: requestOptions.sourceHubspotDealId
        }
      });
    }

    return jsonResponse(500, { error: message });
  }
});
