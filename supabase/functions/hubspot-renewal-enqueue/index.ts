/// <reference types="https://esm.sh/@supabase/functions-js/src/edge-runtime.d.ts" />

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

type RuntimeConfig = {
  ingestSecret: string;
  supabaseUrl: string;
  supabaseServiceRoleKey: string;
  hubspotToken: string;
  companyYouniumCustIdProperty: string;
};

type HubSpotDeal = {
  id: string;
  properties: Record<string, unknown>;
};

type YouniumSubscriptionRow = {
  subscription_id: string;
  account_number: string | null;
  status: string | null;
  effective_start_date: string | null;
  effective_end_date: string | null;
};

type LedgerRow = {
  subscription_id: string;
  term_end_date: string;
  status: string | null;
  source_hubspot_deal_id: string | null;
  hubspot_deal_id: string | null;
  metadata: unknown;
};

type BusinessResult = {
  result:
    | 'queued_new'
    | 'already_queued'
    | 'already_created'
    | 'requeued_from_error'
    | 'not_found'
    | 'invalid_request';
  message: string;
  source_hubspot_deal_id: string;
  company_id?: string;
  younium_custid?: string;
  subscription_id?: string;
  term_end_date?: string;
  existing_hubspot_deal_id?: string;
};

type InternalFunctionCallResult = {
  status: number;
  body: unknown;
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

function loadConfig(): RuntimeConfig {
  return {
    ingestSecret: getRequiredEnv('INGEST_SECRET'),
    supabaseUrl: getRequiredEnv('SUPABASE_URL'),
    supabaseServiceRoleKey: getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY'),
    hubspotToken: getRequiredEnv('HUBSPOT_PRIVATE_APP_TOKEN'),
    companyYouniumCustIdProperty: Deno.env.get('HUBSPOT_COMPANY_YOUNIUM_CUSTID_PROP')?.trim() || 'younium_custid'
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
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

function getHubSpotHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    Accept: 'application/json'
  };
}

function extractHubSpotMessage(body: unknown, fallback: string): string {
  if (isRecord(body)) {
    const message = asNonEmptyString(body.message);
    if (message) {
      return message;
    }
  }

  return fallback;
}

function getIngestSecretFromHeaders(headers: Headers): string | null {
  return headers.get('x-ingest-secret') ?? headers.get('x_ingest_secret');
}

function getFunctionsBaseUrl(supabaseUrl: string): string {
  const configured = Deno.env.get('SUPABASE_FUNCTIONS_BASE_URL')?.trim();
  if (configured) {
    return configured.replace(/\/$/, '');
  }

  return `${supabaseUrl.replace(/\/$/, '')}/functions/v1`;
}

async function invokeInternalFunction(
  functionsBaseUrl: string,
  functionName: string,
  ingestSecret: string,
  payload: Record<string, unknown>
): Promise<InternalFunctionCallResult> {
  const response = await fetch(`${functionsBaseUrl}/${functionName}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-ingest-secret': ingestSecret,
      'x_ingest_secret': ingestSecret
    },
    body: JSON.stringify(payload)
  });

  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text || null;
  }

  return {
    status: response.status,
    body
  };
}

function getRecordNumberValue(payload: unknown, key: string): number | null {
  if (!isRecord(payload)) {
    return null;
  }

  const value = payload[key];
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function getFirstCreatedDealId(payload: unknown): string | null {
  if (!isRecord(payload) || !Array.isArray(payload.results) || payload.results.length === 0) {
    return null;
  }

  const first = payload.results[0];
  if (!isRecord(first)) {
    return null;
  }

  return asNonEmptyString(first.created_deal_id);
}

function parseDealIdFromRequest(payload: unknown): string | null {
  if (!isRecord(payload)) {
    return null;
  }

  return (
    asDealIdString(payload.source_hubspot_deal_id) ??
    asDealIdString(payload.sourceDealId) ??
    asDealIdString(payload.deal_id) ??
    asDealIdString(payload.dealId) ??
    asDealIdString(payload.hs_object_id) ??
    asDealIdString(payload.objectId)
  );
}

async function parseIncomingDealId(req: Request): Promise<string | null> {
  const text = await req.text();
  if (!text.trim()) {
    return null;
  }

  let payload: unknown;
  try {
    payload = JSON.parse(text);
  } catch {
    return null;
  }

  return parseDealIdFromRequest(payload);
}

async function fetchHubSpotDeal(token: string, sourceDealId: string): Promise<HubSpotDeal> {
  const url = new URL(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(sourceDealId)}`);
  url.searchParams.set(
    'properties',
    'dealname,younium_order_effective_start_date,contract_start_date__c,contract_end_date__c,closedate'
  );

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: getHubSpotHeaders(token)
  });

  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }

  if (!response.ok) {
    const message = extractHubSpotMessage(body, text || `HTTP ${response.status}`);
    throw new Error(`Failed to fetch source deal ${sourceDealId}: ${message}`);
  }

  if (!isRecord(body)) {
    throw new Error(`Source deal ${sourceDealId} response missing body`);
  }

  const id = asNonEmptyString(body.id);
  const properties = isRecord(body.properties) ? body.properties : {};
  if (!id) {
    throw new Error(`Source deal ${sourceDealId} response missing id`);
  }

  return { id, properties };
}

async function fetchAssociatedCompanyId(token: string, sourceDealId: string): Promise<string | null> {
  const response = await fetch(
    `https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(sourceDealId)}/associations/companies`,
    {
      method: 'GET',
      headers: getHubSpotHeaders(token)
    }
  );

  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }

  if (!response.ok) {
    const message = extractHubSpotMessage(body, text || `HTTP ${response.status}`);
    throw new Error(`Failed to fetch associated company for deal ${sourceDealId}: ${message}`);
  }

  if (!isRecord(body) || !Array.isArray(body.results) || body.results.length === 0) {
    return null;
  }

  const first = body.results[0];
  if (!isRecord(first)) {
    return null;
  }

  return asNonEmptyString(first.id);
}

async function fetchCompanyYouniumCustId(
  token: string,
  companyId: string,
  propertyName: string
): Promise<string | null> {
  const url = new URL(`https://api.hubapi.com/crm/v3/objects/companies/${encodeURIComponent(companyId)}`);
  url.searchParams.set('properties', propertyName);

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: getHubSpotHeaders(token)
  });

  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }

  if (!response.ok) {
    const message = extractHubSpotMessage(body, text || `HTTP ${response.status}`);
    throw new Error(`Failed to fetch company ${companyId}: ${message}`);
  }

  if (!isRecord(body) || !isRecord(body.properties)) {
    return null;
  }

  return asNonEmptyString(body.properties[propertyName]);
}

function parseDateLike(value: unknown): Date | null {
  const str = asNonEmptyString(value);
  if (!str) {
    return null;
  }

  const trimmed = str.trim();

  if (/^\d+$/.test(trimmed)) {
    const millis = Number.parseInt(trimmed, 10);
    if (!Number.isFinite(millis)) {
      return null;
    }

    const date = new Date(millis);
    return Number.isFinite(date.getTime()) ? date : null;
  }

  const parsed = Date.parse(trimmed);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return new Date(parsed);
}

function dateOnlyUtc(date: Date): string {
  const yyyy = String(date.getUTCFullYear());
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function pickDealAnchorDate(properties: Record<string, unknown>): Date | null {
  return (
    parseDateLike(properties.younium_order_effective_start_date) ??
    parseDateLike(properties.contract_start_date__c) ??
    parseDateLike(properties.contract_end_date__c) ??
    parseDateLike(properties.closedate)
  );
}

function chooseSubscription(
  rows: YouniumSubscriptionRow[],
  anchorDate: Date | null
): YouniumSubscriptionRow | null {
  const validRows = rows.filter((row) => asNonEmptyString(row.subscription_id) && asNonEmptyString(row.effective_end_date));
  if (validRows.length === 0) {
    return null;
  }

  if (!anchorDate) {
    return validRows.sort((a, b) => {
      const aEnd = Date.parse(a.effective_end_date ?? '');
      const bEnd = Date.parse(b.effective_end_date ?? '');
      return bEnd - aEnd;
    })[0];
  }

  const anchorMs = anchorDate.getTime();

  const containing = validRows.filter((row) => {
    const startMs = Date.parse(row.effective_start_date ?? '');
    const endMs = Date.parse(row.effective_end_date ?? '');
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
      return false;
    }

    return anchorMs >= startMs && anchorMs <= endMs;
  });

  if (containing.length > 0) {
    return containing.sort((a, b) => {
      const aEnd = Date.parse(a.effective_end_date ?? '');
      const bEnd = Date.parse(b.effective_end_date ?? '');
      return bEnd - aEnd;
    })[0];
  }

  return validRows.sort((a, b) => {
    const aEnd = Date.parse(a.effective_end_date ?? '');
    const bEnd = Date.parse(b.effective_end_date ?? '');
    const aDistance = Math.abs(aEnd - anchorMs);
    const bDistance = Math.abs(bEnd - anchorMs);
    return aDistance - bDistance;
  })[0];
}

function normalizeMetadata(metadata: unknown): Record<string, unknown> {
  return isRecord(metadata) ? { ...metadata } : {};
}

async function loadLedgerRow(
  supabase: ReturnType<typeof createClient>,
  subscriptionId: string,
  termEndDate: string
): Promise<LedgerRow | null> {
  const { data, error } = await supabase
    .from('renewal_ledger')
    .select('subscription_id,term_end_date,status,source_hubspot_deal_id,hubspot_deal_id,metadata')
    .eq('subscription_id', subscriptionId)
    .eq('term_end_date', termEndDate)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to read renewal_ledger: ${error.message}`);
  }

  return (data ?? null) as LedgerRow | null;
}

async function loadMostRecentCreatedBySourceDeal(
  supabase: ReturnType<typeof createClient>,
  sourceDealId: string
): Promise<LedgerRow | null> {
  const { data, error } = await supabase
    .from('renewal_ledger')
    .select('subscription_id,term_end_date,status,source_hubspot_deal_id,hubspot_deal_id,metadata,updated_at')
    .eq('source_hubspot_deal_id', sourceDealId)
    .not('hubspot_deal_id', 'is', null)
    .order('updated_at', { ascending: false })
    .limit(1);

  if (error) {
    throw new Error(`Failed to read renewal_ledger by source_hubspot_deal_id: ${error.message}`);
  }

  if (!Array.isArray(data) || data.length === 0) {
    return null;
  }

  return data[0] as LedgerRow;
}

async function findExistingHubSpotRenewalDealId(
  token: string,
  sourceDealId: string
): Promise<string | null> {
  const response = await fetch('https://api.hubapi.com/crm/v3/objects/deals/search', {
    method: 'POST',
    headers: {
      ...getHubSpotHeaders(token),
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      filterGroups: [
        {
          filters: [
            {
              propertyName: 'original_deal_id',
              operator: 'EQ',
              value: sourceDealId
            }
          ]
        }
      ],
      properties: ['original_deal_id'],
      limit: 10
    })
  });

  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }

  if (!response.ok) {
    const message = extractHubSpotMessage(body, text || `HTTP ${response.status}`);
    throw new Error(`Failed to search existing HubSpot renewal for source deal ${sourceDealId}: ${message}`);
  }

  if (!isRecord(body) || !Array.isArray(body.results) || body.results.length === 0) {
    return null;
  }

  for (const row of body.results) {
    if (!isRecord(row)) {
      continue;
    }

    const foundId = asNonEmptyString(row.id);
    if (!foundId || foundId === sourceDealId) {
      continue;
    }

    return foundId;
  }

  return null;
}

async function markLedgerAsCreated(
  supabase: ReturnType<typeof createClient>,
  input: {
    subscriptionId: string;
    termEndDate: string;
    sourceDealId: string;
    hubspotDealId: string;
  }
): Promise<void> {
  const { subscriptionId, termEndDate, sourceDealId, hubspotDealId } = input;
  const nowIso = new Date().toISOString();

  const existing = await loadLedgerRow(supabase, subscriptionId, termEndDate);
  const existingMetadata = normalizeMetadata(existing?.metadata);

  const mergedMetadata: Record<string, unknown> = {
    ...existingMetadata,
    source_hubspot_deal_id: sourceDealId,
    discovered_existing_hubspot_deal_id: hubspotDealId,
    discovered_existing_hubspot_deal_at: nowIso
  };

  if (existing) {
    const { error } = await supabase
      .from('renewal_ledger')
      .update({
        status: 'created',
        source_hubspot_deal_id: sourceDealId,
        hubspot_deal_id: hubspotDealId,
        metadata: mergedMetadata,
        updated_at: nowIso
      })
      .eq('subscription_id', subscriptionId)
      .eq('term_end_date', termEndDate);

    if (error) {
      throw new Error(`Failed to mark renewal_ledger row as created: ${error.message}`);
    }

    return;
  }

  const { error } = await supabase.from('renewal_ledger').insert({
    subscription_id: subscriptionId,
    term_end_date: termEndDate,
    status: 'created',
    created_source: 'manual',
    source_hubspot_deal_id: sourceDealId,
    hubspot_deal_id: hubspotDealId,
    metadata: mergedMetadata,
    updated_at: nowIso
  });

  if (error) {
    throw new Error(`Failed to insert created renewal_ledger row: ${error.message}`);
  }
}

async function upsertManualQueueRow(
  supabase: ReturnType<typeof createClient>,
  input: {
    subscriptionId: string;
    termEndDate: string;
    sourceDealId: string;
    companyId: string;
    youniumCustId: string;
    existingRow: LedgerRow | null;
  }
): Promise<BusinessResult> {
  const { subscriptionId, termEndDate, sourceDealId, companyId, youniumCustId, existingRow } = input;
  const nowIso = new Date().toISOString();

  if (existingRow && asNonEmptyString(existingRow.hubspot_deal_id)) {
    return {
      result: 'already_created',
      message: 'Renewal already exists. No duplicate was created.',
      source_hubspot_deal_id: sourceDealId,
      company_id: companyId,
      younium_custid: youniumCustId,
      subscription_id: subscriptionId,
      term_end_date: termEndDate,
      existing_hubspot_deal_id: existingRow.hubspot_deal_id ?? undefined
    };
  }

  const metadata = normalizeMetadata(existingRow?.metadata);
  const mergedMetadata: Record<string, unknown> = {
    ...metadata,
    manual_enqueue_last_requested_at: nowIso,
    manual_enqueue_source_hubspot_deal_id: sourceDealId,
    manual_enqueue_company_id: companyId,
    manual_enqueue_younium_custid: youniumCustId
  };

  if (!existingRow) {
    const { error } = await supabase.from('renewal_ledger').insert({
      subscription_id: subscriptionId,
      term_end_date: termEndDate,
      status: 'planned',
      created_source: 'manual',
      source_hubspot_deal_id: sourceDealId,
      metadata: mergedMetadata,
      updated_at: nowIso
    });

    if (error) {
      throw new Error(`Failed to insert renewal_ledger row: ${error.message}`);
    }

    return {
      result: 'queued_new',
      message: 'Renewal queued successfully.',
      source_hubspot_deal_id: sourceDealId,
      company_id: companyId,
      younium_custid: youniumCustId,
      subscription_id: subscriptionId,
      term_end_date: termEndDate
    };
  }

  const status = (asNonEmptyString(existingRow.status) ?? '').toLowerCase();

  if (status === 'planned' || status === 'processing') {
    const { error } = await supabase
      .from('renewal_ledger')
      .update({
        source_hubspot_deal_id: sourceDealId,
        metadata: mergedMetadata,
        updated_at: nowIso
      })
      .eq('subscription_id', subscriptionId)
      .eq('term_end_date', termEndDate);

    if (error) {
      throw new Error(`Failed to update queued renewal_ledger row: ${error.message}`);
    }

    return {
      result: 'already_queued',
      message: 'Renewal was already queued. No duplicate was created.',
      source_hubspot_deal_id: sourceDealId,
      company_id: companyId,
      younium_custid: youniumCustId,
      subscription_id: subscriptionId,
      term_end_date: termEndDate
    };
  }

  if (status === 'error') {
    const { error } = await supabase
      .from('renewal_ledger')
      .update({
        status: 'planned',
        source_hubspot_deal_id: sourceDealId,
        metadata: mergedMetadata,
        updated_at: nowIso
      })
      .eq('subscription_id', subscriptionId)
      .eq('term_end_date', termEndDate);

    if (error) {
      throw new Error(`Failed to requeue errored renewal_ledger row: ${error.message}`);
    }

    return {
      result: 'requeued_from_error',
      message: 'Renewal was previously in error and has been re-queued.',
      source_hubspot_deal_id: sourceDealId,
      company_id: companyId,
      younium_custid: youniumCustId,
      subscription_id: subscriptionId,
      term_end_date: termEndDate
    };
  }

  const { error } = await supabase
    .from('renewal_ledger')
    .update({
      status: 'planned',
      source_hubspot_deal_id: sourceDealId,
      metadata: mergedMetadata,
      updated_at: nowIso
    })
    .eq('subscription_id', subscriptionId)
    .eq('term_end_date', termEndDate);

  if (error) {
    throw new Error(`Failed to upsert renewal_ledger row: ${error.message}`);
  }

  return {
    result: 'already_queued',
    message: 'Renewal was found and set to queued.',
    source_hubspot_deal_id: sourceDealId,
    company_id: companyId,
    younium_custid: youniumCustId,
    subscription_id: subscriptionId,
    term_end_date: termEndDate
  };
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

  const providedSecret = getIngestSecretFromHeaders(req.headers);
  if (!providedSecret || providedSecret !== config.ingestSecret) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  const sourceDealId = await parseIncomingDealId(req);
  if (!sourceDealId) {
    return jsonResponse(200, {
      result: 'invalid_request',
      message: 'Missing source_hubspot_deal_id in request body.',
      timestamp: new Date().toISOString()
    });
  }

  try {
    const supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
      auth: { persistSession: false }
    });

    const sourceDeal = await fetchHubSpotDeal(config.hubspotToken, sourceDealId);
    const companyId = await fetchAssociatedCompanyId(config.hubspotToken, sourceDealId);

    if (!companyId) {
      return jsonResponse(200, {
        result: 'not_found',
        message: 'No associated company found on source deal.',
        source_hubspot_deal_id: sourceDealId,
        timestamp: new Date().toISOString()
      });
    }

    const youniumCustId = await fetchCompanyYouniumCustId(
      config.hubspotToken,
      companyId,
      config.companyYouniumCustIdProperty
    );

    if (!youniumCustId) {
      return jsonResponse(200, {
        result: 'not_found',
        message: `Company is missing ${config.companyYouniumCustIdProperty}.`,
        source_hubspot_deal_id: sourceDealId,
        company_id: companyId,
        timestamp: new Date().toISOString()
      });
    }

    const anchorDate = pickDealAnchorDate(sourceDeal.properties);

    const { data: subscriptionsData, error: subscriptionsError } = await supabase
      .from('younium_subscriptions')
      .select('subscription_id,account_number,status,effective_start_date,effective_end_date')
      .eq('account_number', youniumCustId);

    if (subscriptionsError) {
      throw new Error(`Failed to query younium_subscriptions: ${subscriptionsError.message}`);
    }

    const subscriptionRows = (subscriptionsData ?? []) as YouniumSubscriptionRow[];
    const selected = chooseSubscription(subscriptionRows, anchorDate);

    if (!selected || !selected.effective_end_date) {
      return jsonResponse(200, {
        result: 'not_found',
        message: 'No matching Younium subscription found for this company account.',
        source_hubspot_deal_id: sourceDealId,
        company_id: companyId,
        younium_custid: youniumCustId,
        timestamp: new Date().toISOString()
      });
    }

    const effectiveEndDate = parseDateLike(selected.effective_end_date);
    if (!effectiveEndDate) {
      return jsonResponse(200, {
        result: 'not_found',
        message: 'Selected Younium subscription has an invalid effective_end_date.',
        source_hubspot_deal_id: sourceDealId,
        company_id: companyId,
        younium_custid: youniumCustId,
        timestamp: new Date().toISOString()
      });
    }

    const subscriptionId = selected.subscription_id;
    const termEndDate = dateOnlyUtc(effectiveEndDate);
    const existingLedger = await loadLedgerRow(supabase, subscriptionId, termEndDate);

    if (existingLedger && asNonEmptyString(existingLedger.hubspot_deal_id)) {
      return jsonResponse(200, {
        result: 'already_created',
        message: 'Renewal already exists. No duplicate was created.',
        source_hubspot_deal_id: sourceDealId,
        company_id: companyId,
        younium_custid: youniumCustId,
        subscription_id: subscriptionId,
        term_end_date: termEndDate,
        existing_hubspot_deal_id: existingLedger.hubspot_deal_id ?? undefined,
        timestamp: new Date().toISOString()
      });
    }

    const existingCreatedBySource = await loadMostRecentCreatedBySourceDeal(supabase, sourceDealId);
    if (existingCreatedBySource && asNonEmptyString(existingCreatedBySource.hubspot_deal_id)) {
      return jsonResponse(200, {
        result: 'already_created',
        message: 'Renewal already exists. No duplicate was created.',
        source_hubspot_deal_id: sourceDealId,
        company_id: companyId,
        younium_custid: youniumCustId,
        subscription_id: existingCreatedBySource.subscription_id,
        term_end_date: existingCreatedBySource.term_end_date,
        existing_hubspot_deal_id: existingCreatedBySource.hubspot_deal_id ?? undefined,
        timestamp: new Date().toISOString()
      });
    }

    const existingHubspotDealId = await findExistingHubSpotRenewalDealId(config.hubspotToken, sourceDealId);
    if (existingHubspotDealId) {
      await markLedgerAsCreated(supabase, {
        subscriptionId,
        termEndDate,
        sourceDealId,
        hubspotDealId: existingHubspotDealId
      });

      return jsonResponse(200, {
        result: 'already_created',
        message: 'Renewal already exists. No duplicate was created.',
        source_hubspot_deal_id: sourceDealId,
        company_id: companyId,
        younium_custid: youniumCustId,
        subscription_id: subscriptionId,
        term_end_date: termEndDate,
        existing_hubspot_deal_id: existingHubspotDealId,
        timestamp: new Date().toISOString()
      });
    }

    const result = await upsertManualQueueRow(supabase, {
      subscriptionId,
      termEndDate,
      sourceDealId,
      companyId,
      youniumCustId,
      existingRow: existingLedger
    });

    if (result.result === 'already_created') {
      return jsonResponse(200, {
        ...result,
        timestamp: new Date().toISOString()
      });
    }

    const functionsBaseUrl = getFunctionsBaseUrl(config.supabaseUrl);

    const snapshotCall = await invokeInternalFunction(
      functionsBaseUrl,
      'renewal-snapshot',
      config.ingestSecret,
      {
        source_hubspot_deal_id: sourceDealId
      }
    );

    const createCall = await invokeInternalFunction(
      functionsBaseUrl,
      'hubspot-renewal-create',
      config.ingestSecret,
      {
        source_hubspot_deal_id: sourceDealId,
        create_line_items: true
      }
    );

    const createdCount = getRecordNumberValue(createCall.body, 'created') ?? 0;
    const processedCount = getRecordNumberValue(createCall.body, 'processed') ?? 0;
    const createErrors = getRecordNumberValue(createCall.body, 'errors') ?? 0;
    const createdDealId = getFirstCreatedDealId(createCall.body);

    let message = result.message;
    if (createCall.status === 200 && createdCount > 0) {
      message = 'Renewal queued and created immediately.';
    } else if (createCall.status === 200 && processedCount === 0) {
      message = 'Renewal queued, but no ready row was found for immediate creation. It will run in scheduled automation.';
    } else if (createCall.status !== 200 || createErrors > 0) {
      message = 'Renewal queued, but immediate creation hit an error. It will retry in scheduled automation.';
    }

    return jsonResponse(200, {
      ...result,
      message,
      immediate_run: {
        snapshot_status: snapshotCall.status,
        snapshot_response: snapshotCall.body,
        create_status: createCall.status,
        create_response: createCall.body,
        created_count: createdCount,
        processed_count: processedCount,
        errors_count: createErrors,
        created_deal_id: createdDealId
      },
      timestamp: new Date().toISOString()
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, {
      error: message,
      source_hubspot_deal_id: sourceDealId,
      timestamp: new Date().toISOString()
    });
  }
});
