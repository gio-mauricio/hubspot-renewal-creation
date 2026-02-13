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
  hubspotToken: string;
  forecastPipelineId: string;
  forecastDealstageId: string;
  runMode: string;
  lineItemPropertyEnv: Record<string, string | null>;
};

type ReadyRow = {
  subscription_id: string;
  term_end_date: string;
  source_hubspot_deal_id: string;
  younium_charges_json?: unknown;
};

type RowResult = {
  subscription_id: string;
  term_end_date: string;
  source_hubspot_deal_id: string;
  created_deal_id?: string;
  calculated_amount?: string;
  status_note?: string;
  line_items_created_count: number;
  line_items_deduped_count: number;
  line_items_error_count: number;
  line_item_ids: string[];
  line_item_error_samples: string[];
  would_create?: {
    deal_payload: Record<string, string>;
    line_item_property_payloads_sample?: Array<Record<string, string>>;
    fingerprint_examples?: string[];
  };
  error?: string;
};

type SourceDealProperties = {
  dealname?: string;
  dealstage?: string;
  pipeline?: string;
  amount?: string;
  closedate?: string;
  hubspot_owner_id?: string;
  [key: string]: unknown;
};

type RequestOptions = {
  limit: number | null;
  dryRun: boolean;
  createLineItems: boolean;
  sourceHubspotDealId: string | null;
};

type ChargeBuildResult =
  | {
      ok: true;
      chargeIdentifier: string;
      fingerprint: string;
      properties: Record<string, string>;
    }
  | {
      ok: false;
      message: string;
    };

type LineItemRunSummary = {
  createdCount: number;
  dedupedCount: number;
  errorCount: number;
  lineItemIds: string[];
  associatedLineItemIds: string[];
  errorSamples: string[];
  dryRunPayloadSamples: Array<Record<string, string>>;
  dryRunFingerprintSamples: string[];
};

const REQUIRED_LINE_ITEM_PROPERTY_ENV_NAMES = [
  'HS_LI_YOUNIUM_CHARGE_EFFECTIVE_START_DATE_PROP',
  'HS_LI_YOUNIUM_CHARGE_EFFECTIVE_END_DATE_PROP',
  'HS_LI_YOUNIUM_LINE_ITEM_STATUS_PROP',
  'HS_LI_YOUNIUM_ORDER_PRODUCT_CHARGE_PROP',
  'HS_LI_YOUNIUM_START_ON_PROP',
  'HS_LI_YOUNIUM_END_ON_PROP',
  'HS_LI_FINGERPRINT_PROP',
  'HS_LI_YOUNIUM_CHARGE_ID_PROP',
  'HS_LI_YOUNIUM_ORDER_CHARGE_ID_PROP'
] as const;

const DIRECT_COPY_FIELDS = [
  'billing_contact_name',
  'billing_title',
  'billing_phone',
  'billing_email',
  'billing_notes',
  'younium_remarks',
  'younium_order_number',
  'younium_owner',
  'deal_company_domain'
] as const;

const SELECT_COPY_FIELDS = [
  'renewal_billing_status',
  'deal_term',
  'payment_frequency',
  'payment_terms',
  'autorenew'
] as const;

const MULTISELECT_COPY_FIELDS = [
  'crm_',
  'custom_directory',
  'email_server_',
  'marketing_automation_system_multi_select',
  'security_software',
  'sso_'
] as const;

const SOURCE_DEAL_PROPERTIES = [
  'dealname',
  'dealstage',
  'pipeline',
  'amount',
  'closedate',
  'hubspot_owner_id',
  'cs_owner_2',
  ...DIRECT_COPY_FIELDS,
  ...SELECT_COPY_FIELDS,
  ...MULTISELECT_COPY_FIELDS,
  'contract_start_date__c',
  'contract_end_date__c',
  'younium_order_effective_start_date',
  'younium_order_effective_change_date',
  'younium_initial_term',
  'younium_renewal_term'
] as const;

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

function getOptionalEnv(name: string): string | null {
  const value = Deno.env.get(name)?.trim();
  return value && value.length > 0 ? value : null;
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

function asFiniteNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const parsed = Number.parseFloat(value.trim());
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return null;
}

function getPropertyString(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }

  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }

  return null;
}

function parseIsoDateToUtcDate(value: string, fieldName: string): Date {
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    throw new Error(`Invalid ${fieldName} format: ${value}`);
  }

  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);

  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new Error(`Invalid ${fieldName} value: ${value}`);
  }

  return date;
}

function parseDateLikeToUtcDate(value: string, fieldName: string): Date {
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) {
    const ms = Number.parseInt(trimmed, 10);
    if (!Number.isFinite(ms)) {
      throw new Error(`Invalid ${fieldName}: ${value}`);
    }

    const parsedDate = new Date(ms);
    if (!Number.isFinite(parsedDate.getTime())) {
      throw new Error(`Invalid ${fieldName}: ${value}`);
    }

    return new Date(
      Date.UTC(parsedDate.getUTCFullYear(), parsedDate.getUTCMonth(), parsedDate.getUTCDate())
    );
  }

  const isoPrefixMatch = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoPrefixMatch) {
    return parseIsoDateToUtcDate(
      `${isoPrefixMatch[1]}-${isoPrefixMatch[2]}-${isoPrefixMatch[3]}`,
      fieldName
    );
  }

  const parsedMs = Date.parse(trimmed);
  if (!Number.isFinite(parsedMs)) {
    throw new Error(`Invalid ${fieldName}: ${value}`);
  }

  const parsedDate = new Date(parsedMs);
  return new Date(
    Date.UTC(parsedDate.getUTCFullYear(), parsedDate.getUTCMonth(), parsedDate.getUTCDate())
  );
}

function addUtcDays(date: Date, days: number): Date {
  const next = new Date(date.getTime());
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function addUtcMonths(date: Date, months: number): Date {
  const targetMonthStart = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + months, 1));
  const lastDayInTargetMonth = new Date(
    Date.UTC(targetMonthStart.getUTCFullYear(), targetMonthStart.getUTCMonth() + 1, 0)
  ).getUTCDate();
  const day = Math.min(date.getUTCDate(), lastDayInTargetMonth);

  return new Date(Date.UTC(targetMonthStart.getUTCFullYear(), targetMonthStart.getUTCMonth(), day));
}

function formatUtcDateYYYYMMDD(date: Date): string {
  const yyyy = String(date.getUTCFullYear());
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function formatUtcDateMMDDYYYY(date: Date): string {
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  const yyyy = String(date.getUTCFullYear());
  return `${mm}/${dd}/${yyyy}`;
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

async function fetchCompanyDomain(token: string, companyId: string): Promise<string | null> {
  const url = new URL(`https://api.hubapi.com/crm/v3/objects/companies/${encodeURIComponent(companyId)}`);
  url.searchParams.set('properties', 'domain');

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

  return asNonEmptyString(body.properties.domain);
}

async function fetchSourceDealCompanyDomain(token: string, sourceDealId: string): Promise<string | null> {
  const companyId = await fetchAssociatedCompanyId(token, sourceDealId);
  if (!companyId) {
    return null;
  }

  return fetchCompanyDomain(token, companyId);
}

function parseLimit(raw: unknown): number | null {
  if (raw == null) {
    return null;
  }

  if (typeof raw !== 'number' || !Number.isFinite(raw)) {
    throw new Error('Invalid request body: "limit" must be a number');
  }

  if (raw <= 0) {
    throw new Error('Invalid request body: "limit" must be greater than 0');
  }

  return Math.floor(raw);
}

async function getRequestOptions(req: Request): Promise<RequestOptions> {
  const text = await req.text();

  if (!text.trim()) {
    return {
      limit: null,
      dryRun: false,
      createLineItems: false,
      sourceHubspotDealId: null
    };
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

  const dryRunRaw = parsed.dry_run;
  if (dryRunRaw != null && typeof dryRunRaw !== 'boolean') {
    throw new Error('Invalid request body: "dry_run" must be a boolean');
  }

  const createLineItemsRaw = parsed.create_line_items;
  if (createLineItemsRaw != null && typeof createLineItemsRaw !== 'boolean') {
    throw new Error('Invalid request body: "create_line_items" must be a boolean');
  }

  const sourceHubspotDealIdRaw =
    asDealIdString(parsed.source_hubspot_deal_id) ??
    asDealIdString(parsed.sourceDealId) ??
    asDealIdString(parsed.deal_id) ??
    asDealIdString(parsed.dealId) ??
    asDealIdString(parsed.hs_object_id) ??
    null;

  return {
    limit: parseLimit(parsed.limit),
    dryRun: dryRunRaw === true,
    createLineItems: createLineItemsRaw === true,
    sourceHubspotDealId: sourceHubspotDealIdRaw
  };
}

function termEndDateToEpochMs(termEndDate: string): string {
  const ms = Date.parse(`${termEndDate}T00:00:00.000Z`);

  if (!Number.isFinite(ms)) {
    throw new Error(`Invalid term_end_date: ${termEndDate}`);
  }

  return String(ms);
}

function getTermMonthsFromDealTerm(dealTermRaw: string | null): number {
  if (!dealTermRaw) {
    return 12;
  }

  const normalized = dealTermRaw.toLowerCase().replace(/[\s_-]+/g, '');
  if (normalized === '2year') {
    return 24;
  }
  if (normalized === '3year') {
    return 36;
  }

  return 12;
}

function calculateForecastDates(
  sourceProperties: SourceDealProperties,
  row: ReadyRow
): {
  forecastStart: Date;
  forecastEnd: Date;
  termMonths: number;
} {
  const termMonths = getTermMonthsFromDealTerm(asNonEmptyString(sourceProperties.deal_term));

  const sourceYouniumStartRaw = getPropertyString(sourceProperties, 'younium_order_effective_start_date');
  const sourceContractEndRaw = getPropertyString(sourceProperties, 'contract_end_date__c');

  let forecastStart: Date;

  if (sourceYouniumStartRaw) {
    const sourceYouniumStart = parseDateLikeToUtcDate(
      sourceYouniumStartRaw,
      'source younium_order_effective_start_date'
    );
    forecastStart = addUtcMonths(sourceYouniumStart, termMonths);
  } else if (sourceContractEndRaw) {
    const sourceContractEnd = parseDateLikeToUtcDate(
      sourceContractEndRaw,
      'source contract_end_date__c'
    );
    forecastStart = addUtcDays(sourceContractEnd, 1);
  } else {
    const ledgerTermEnd = parseIsoDateToUtcDate(row.term_end_date, 'term_end_date');
    forecastStart = addUtcDays(ledgerTermEnd, 1);
  }

  const forecastEnd = addUtcDays(addUtcMonths(forecastStart, termMonths), -1);

  return {
    forecastStart,
    forecastEnd,
    termMonths
  };
}

function loadConfig(): RuntimeConfig {
  const hubspotToken = Deno.env.get('HUBSPOT_PRIVATE_APP_TOKEN')?.trim();
  if (!hubspotToken) {
    throw new Error('Missing HUBSPOT_PRIVATE_APP_TOKEN');
  }

  const forecastPipelineId = Deno.env.get('FORECAST_PIPELINE_ID')?.trim();
  if (!forecastPipelineId) {
    throw new Error('Missing FORECAST_PIPELINE_ID');
  }

  const forecastDealstageId = Deno.env.get('FORECAST_DEALSTAGE_ID')?.trim();
  if (!forecastDealstageId) {
    throw new Error('Missing FORECAST_DEALSTAGE_ID');
  }

  const lineItemPropertyEnv: Record<string, string | null> = {};
  for (const envName of REQUIRED_LINE_ITEM_PROPERTY_ENV_NAMES) {
    lineItemPropertyEnv[envName] = getOptionalEnv(envName);
  }

  return {
    ingestSecret: getRequiredEnv('INGEST_SECRET'),
    supabaseUrl: getRequiredEnv('SUPABASE_URL'),
    supabaseServiceRoleKey: getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY'),
    hubspotToken,
    forecastPipelineId,
    forecastDealstageId,
    runMode: Deno.env.get('RUN_MODE')?.trim() ?? '',
    lineItemPropertyEnv
  };
}

function getMissingLineItemEnvVars(config: RuntimeConfig): string[] {
  return REQUIRED_LINE_ITEM_PROPERTY_ENV_NAMES.filter((name) => !config.lineItemPropertyEnv[name]);
}

function getHubSpotHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    Accept: 'application/json'
  };
}

function getIngestSecretFromHeaders(headers: Headers): string | null {
  return headers.get('x-ingest-secret') ?? headers.get('x_ingest_secret');
}

function parseCharges(payload: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(payload)) {
    return payload.filter((item): item is Record<string, unknown> => isRecord(item));
  }

  if (isRecord(payload)) {
    if (Array.isArray(payload.items)) {
      return payload.items.filter((item): item is Record<string, unknown> => isRecord(item));
    }

    if (Array.isArray(payload.data)) {
      return payload.data.filter((item): item is Record<string, unknown> => isRecord(item));
    }

    if (Array.isArray(payload.value)) {
      return payload.value.filter((item): item is Record<string, unknown> => isRecord(item));
    }
  }

  return [];
}

function isRecurringAndNotCancelled(charge: Record<string, unknown>): boolean {
  const chargeType = asNonEmptyString(charge.chargeType)?.toLowerCase();
  const changeState = asNonEmptyString(charge.changeState)?.toLowerCase();

  if (chargeType !== 'recurring') {
    return false;
  }

  if (!changeState) {
    return false;
  }

  return changeState === 'new' || changeState === 'notchanged' || changeState === 'newfromchange';
}

function findUnitPrice(charge: Record<string, unknown>): number | null {
  const fromDisplayPrice = asFiniteNumber(charge.displayPrice);
  if (fromDisplayPrice != null) {
    return fromDisplayPrice;
  }

  const priceDetails = charge.priceDetails;
  if (Array.isArray(priceDetails) && priceDetails.length > 0 && isRecord(priceDetails[0])) {
    const nested = asFiniteNumber(priceDetails[0].price);
    if (nested != null) {
      return nested;
    }
  }

  return null;
}

function normalizeLookupKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function getNestedString(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  if (isRecord(value)) {
    return asNonEmptyString(value.id) ?? asNonEmptyString(value.name);
  }

  return asNonEmptyString(value);
}

function getChargeTextCandidates(charge: Record<string, unknown>): string[] {
  return [
    asNonEmptyString(charge.name),
    asNonEmptyString(charge.productName),
    getNestedString(charge, 'product')
  ].filter((value): value is string => Boolean(value));
}

function isOpensensePackageCharge(charge: Record<string, unknown>): boolean {
  const targets = new Set([
    'opensensesignaturepackage',
    'opensensepipelinepackage',
    'opensensecompletepackage'
  ]);

  return getChargeTextCandidates(charge).some((value) => targets.has(normalizeLookupKey(value)));
}

function mapChargeToRecurringFrequency(charge: Record<string, unknown>, termMonths: number): string | null {
  if (isOpensensePackageCharge(charge)) {
    return 'annually';
  }

  const pricePeriodRaw = asNonEmptyString(charge.pricePeriod) ?? asNonEmptyString(charge.billingPeriod);
  const pricePeriod = normalizeLookupKey(pricePeriodRaw ?? '');

  if (!pricePeriod) {
    return null;
  }

  if (pricePeriod === 'monthly') {
    return 'monthly';
  }
  if (pricePeriod === 'quarterly') {
    return 'quarterly';
  }
  if (pricePeriod === 'biannual' || pricePeriod === 'semiannually') {
    return 'per_six_months';
  }
  if (pricePeriod === 'annual' || pricePeriod === 'annually') {
    return 'annually';
  }
  if (pricePeriod === 'weekly') {
    return 'weekly';
  }
  if (pricePeriod === 'everytwoweeks' || pricePeriod === 'biweekly') {
    return 'biweekly';
  }
  if (pricePeriod === 'endofterm') {
    if (termMonths >= 60) {
      return 'per_five_years';
    }
    if (termMonths >= 48) {
      return 'per_four_years';
    }
    if (termMonths >= 36) {
      return 'per_three_years';
    }
    if (termMonths >= 24) {
      return 'per_two_years';
    }

    return 'annually';
  }

  return null;
}

function mapChargeToYouniumBillingPeriod(charge: Record<string, unknown>): string {
  if (isOpensensePackageCharge(charge)) {
    return 'annual';
  }

  const pricePeriodRaw = asNonEmptyString(charge.pricePeriod) ?? asNonEmptyString(charge.billingPeriod);
  const normalized = normalizeLookupKey(pricePeriodRaw ?? '');

  if (normalized === 'monthly') {
    return 'monthly';
  }
  if (normalized === 'quarterly') {
    return 'quarterly';
  }
  if (normalized === 'biannual' || normalized === 'semiannually') {
    return 'biannual';
  }
  if (normalized === 'endofterm') {
    return 'endOfTerm';
  }

  return 'annual';
}

function mapChargeToYouniumChargeModel(charge: Record<string, unknown>): string {
  const priceModel = normalizeLookupKey(asNonEmptyString(charge.priceModel) ?? '');
  return priceModel === 'quantity' ? 'quantity' : 'flat';
}

function mapChargeToYouniumChargeType(charge: Record<string, unknown>, chargeName: string): string {
  const chargeType = normalizeLookupKey(asNonEmptyString(charge.chargeType) ?? '');
  if (chargeType === 'oneoff' || chargeType === 'onetime') {
    return 'OneOff';
  }
  if (chargeType === 'recurring') {
    return 'recurring';
  }

  return /one[\s-]?time/i.test(chargeName) ? 'OneOff' : 'recurring';
}

function toHubspotRecurringBillingPeriod(termMonths: number): string {
  return `P${Math.max(1, Math.floor(termMonths))}M`;
}

function buildLineItemProperties(
  charge: Record<string, unknown>,
  lineItemPropNames: Record<string, string>,
  fingerprint: string,
  forecastStartMs: string,
  forecastEndYmd: string,
  termMonths: number
): ChargeBuildResult {
  const chargeId = asNonEmptyString(charge.chargeId) ?? asNonEmptyString(charge.id);
  const orderChargeId = asNonEmptyString(charge.id);
  const chargeNumber = asNonEmptyString(charge.chargeNumber);
  const chargeName = asNonEmptyString(charge.name) ?? chargeNumber ?? chargeId ?? 'Younium Charge';
  const rawQuantity = asFiniteNumber(charge.quantity) ?? 1;
  const quantity = rawQuantity > 0 ? rawQuantity : 1;
  const unitPrice = findUnitPrice(charge);
  const recurringBillingFrequency = mapChargeToRecurringFrequency(charge, termMonths);

  if (!chargeId) {
    return {
      ok: false,
      message: 'Missing charge identifier (chargeId/id)'
    };
  }

  if (!chargeNumber) {
    return {
      ok: false,
      message: `Missing charge.chargeNumber for charge ${chargeId}`
    };
  }

  if (unitPrice == null) {
    return {
      ok: false,
      message: `Missing price for charge ${chargeId}`
    };
  }

  if (!recurringBillingFrequency) {
    const pricePeriod = asNonEmptyString(charge.pricePeriod) ?? asNonEmptyString(charge.billingPeriod) ?? '<missing>';
    return {
      ok: false,
      message: `Unsupported pricePeriod "${pricePeriod}" for charge ${chargeId}`
    };
  }
  const isOpensensePackage = isOpensensePackageCharge(charge);
  const annualizedPackagePrice =
    isOpensensePackage && recurringBillingFrequency === 'annually' ? unitPrice * 12 : unitPrice;
  const unitPricePerQuantity = isOpensensePackage ? annualizedPackagePrice : unitPrice / quantity;
  const youniumBillingPeriod = mapChargeToYouniumBillingPeriod(charge);
  const youniumChargeType = mapChargeToYouniumChargeType(charge, chargeName);
  const youniumChargeModel = mapChargeToYouniumChargeModel(charge);

  const normalizedUnitPrice = unitPricePerQuantity < 0 ? 0 : unitPricePerQuantity;
  const unitDiscount = unitPricePerQuantity < 0 ? Math.abs(unitPricePerQuantity) : null;

  const properties: Record<string, string> = {
    name: chargeName,
    quantity: String(quantity),
    price: String(normalizedUnitPrice),
    hs_recurring_billing_period: toHubspotRecurringBillingPeriod(termMonths),
    recurringbillingfrequency: recurringBillingFrequency,
    younium_billing_period: youniumBillingPeriod,
    younium_charge_model: youniumChargeModel,
    younium_charge_type: youniumChargeType,
    [lineItemPropNames.HS_LI_YOUNIUM_CHARGE_EFFECTIVE_START_DATE_PROP]: forecastStartMs,
    [lineItemPropNames.HS_LI_YOUNIUM_CHARGE_EFFECTIVE_END_DATE_PROP]: forecastEndYmd,
    [lineItemPropNames.HS_LI_YOUNIUM_LINE_ITEM_STATUS_PROP]: 'Existing',
    [lineItemPropNames.HS_LI_YOUNIUM_ORDER_PRODUCT_CHARGE_PROP]: chargeNumber,
    [lineItemPropNames.HS_LI_YOUNIUM_START_ON_PROP]: 'alignToOrder',
    [lineItemPropNames.HS_LI_YOUNIUM_END_ON_PROP]: 'alignToOrder',
    [lineItemPropNames.HS_LI_FINGERPRINT_PROP]: fingerprint,
    [lineItemPropNames.HS_LI_YOUNIUM_CHARGE_ID_PROP]: chargeId,
    [lineItemPropNames.HS_LI_YOUNIUM_ORDER_CHARGE_ID_PROP]: orderChargeId ?? ''
  };

  if (unitDiscount != null && unitDiscount > 0) {
    properties.discount = String(unitDiscount);
  }

  return {
    ok: true,
    chargeIdentifier: chargeId,
    fingerprint,
    properties
  };
}

async function searchLineItemByFingerprint(
  token: string,
  fingerprintPropertyName: string,
  fingerprint: string
): Promise<string | null> {
  const response = await fetch('https://api.hubapi.com/crm/v3/objects/line_items/search', {
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
              propertyName: fingerprintPropertyName,
              operator: 'EQ',
              value: fingerprint
            }
          ]
        }
      ],
      properties: [fingerprintPropertyName],
      limit: 1
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
    throw new Error(`Line item search failed: ${message}`);
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

async function createHubSpotLineItem(token: string, properties: Record<string, string>): Promise<string> {
  const response = await fetch('https://api.hubapi.com/crm/v3/objects/line_items', {
    method: 'POST',
    headers: {
      ...getHubSpotHeaders(token),
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ properties })
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
    throw new Error(`Line item create failed: ${message}`);
  }

  if (!isRecord(body)) {
    throw new Error('Line item create response missing body');
  }

  const lineItemId = asNonEmptyString(body.id);
  if (!lineItemId) {
    throw new Error('Line item create response missing id');
  }

  return lineItemId;
}

async function associateDealToLineItem(token: string, dealId: string, lineItemId: string): Promise<void> {
  const response = await fetch(
    `https://api.hubapi.com/crm/v4/objects/deals/${encodeURIComponent(dealId)}/associations/default/line_items/${encodeURIComponent(lineItemId)}`,
    {
      method: 'PUT',
      headers: getHubSpotHeaders(token)
    }
  );

  if (response.ok || response.status === 409) {
    return;
  }

  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }

  const message = extractHubSpotMessage(body, text || `HTTP ${response.status}`);
  throw new Error(`Association failed: ${message}`);
}

function toHubSpotAmountString(value: number): string {
  if (!Number.isFinite(value)) {
    return '0.00';
  }

  return value.toFixed(2);
}

async function fetchLineItemsNetTcv(token: string, lineItemIds: string[]): Promise<number> {
  if (lineItemIds.length === 0) {
    return 0;
  }

  let total = 0;
  const chunkSize = 100;

  for (let i = 0; i < lineItemIds.length; i += chunkSize) {
    const chunk = lineItemIds.slice(i, i + chunkSize);

    const response = await fetch('https://api.hubapi.com/crm/v3/objects/line_items/batch/read', {
      method: 'POST',
      headers: {
        ...getHubSpotHeaders(token),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        properties: ['hs_tcv'],
        inputs: chunk.map((id) => ({ id }))
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
      throw new Error(`Line item batch read failed: ${message}`);
    }

    if (!isRecord(body) || !Array.isArray(body.results)) {
      continue;
    }

    for (const result of body.results) {
      if (!isRecord(result) || !isRecord(result.properties)) {
        continue;
      }

      const tcv = asFiniteNumber(result.properties.hs_tcv);
      if (tcv != null) {
        total += tcv;
      }
    }
  }

  return total;
}

async function updateDealAmount(token: string, dealId: string, amount: number): Promise<void> {
  const response = await fetch(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(dealId)}`, {
    method: 'PATCH',
    headers: {
      ...getHubSpotHeaders(token),
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      properties: {
        amount: toHubSpotAmountString(amount)
      }
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
    throw new Error(`Deal amount update failed: ${message}`);
  }
}

async function readLedgerMetadata(
  supabase: ReturnType<typeof createClient>,
  row: Pick<ReadyRow, 'subscription_id' | 'term_end_date'>
): Promise<Record<string, unknown>> {
  const { data, error } = await supabase
    .from('renewal_ledger')
    .select('metadata')
    .eq('subscription_id', row.subscription_id)
    .eq('term_end_date', row.term_end_date)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to read renewal_ledger metadata: ${error.message}`);
  }

  const metadata = data?.metadata;
  if (isRecord(metadata)) {
    return { ...metadata };
  }

  return {};
}

async function markLedgerError(
  supabase: ReturnType<typeof createClient>,
  row: ReadyRow,
  message: string,
  extraMetadata?: Record<string, unknown>
): Promise<void> {
  const nowIso = new Date().toISOString();
  const existingMetadata = await readLedgerMetadata(supabase, row);

  const mergedMetadata = {
    ...existingMetadata,
    ...(extraMetadata ?? {}),
    source_hubspot_deal_id: row.source_hubspot_deal_id,
    hubspot_error: message,
    hubspot_error_at: nowIso
  };

  const { error } = await supabase
    .from('renewal_ledger')
    .update({
      status: 'error',
      metadata: mergedMetadata,
      updated_at: nowIso
    })
    .eq('subscription_id', row.subscription_id)
    .eq('term_end_date', row.term_end_date);

  if (error) {
    throw new Error(`Failed to mark renewal_ledger row as error: ${error.message}`);
  }
}

async function releaseLedgerForRetry(
  supabase: ReturnType<typeof createClient>,
  row: ReadyRow,
  message: string
): Promise<void> {
  const nowIso = new Date().toISOString();
  const existingMetadata = await readLedgerMetadata(supabase, row);

  const mergedMetadata = {
    ...existingMetadata,
    source_hubspot_deal_id: row.source_hubspot_deal_id,
    hubspot_error: message,
    hubspot_error_at: nowIso
  };

  const { error } = await supabase
    .from('renewal_ledger')
    .update({
      status: 'planned',
      metadata: mergedMetadata,
      updated_at: nowIso
    })
    .eq('subscription_id', row.subscription_id)
    .eq('term_end_date', row.term_end_date)
    .eq('status', 'processing');

  if (error) {
    throw new Error(`Failed to release renewal_ledger row back to planned: ${error.message}`);
  }
}

async function claimLedgerForProcessing(
  supabase: ReturnType<typeof createClient>,
  row: ReadyRow
): Promise<boolean> {
  const nowIso = new Date().toISOString();
  const { data, error } = await supabase
    .from('renewal_ledger')
    .update({
      status: 'processing',
      updated_at: nowIso
    })
    .eq('subscription_id', row.subscription_id)
    .eq('term_end_date', row.term_end_date)
    .eq('status', 'planned')
    .is('hubspot_deal_id', null)
    .select('subscription_id')
    .limit(1);

  if (error) {
    throw new Error(`Failed to claim renewal_ledger row for processing: ${error.message}`);
  }

  return Array.isArray(data) && data.length > 0;
}

async function processLineItemsForRow(params: {
  config: RuntimeConfig;
  lineItemPropNames: Record<string, string>;
  row: ReadyRow;
  createdDealId: string;
  forecastStartMs: string;
  forecastEndYmd: string;
  termMonths: number;
  dryRun: boolean;
}): Promise<LineItemRunSummary> {
  const { config, lineItemPropNames, row, createdDealId, forecastStartMs, forecastEndYmd, termMonths, dryRun } = params;

  const allCharges = parseCharges(row.younium_charges_json);
  const charges = allCharges.filter(isRecurringAndNotCancelled);

  const lineItemIds: string[] = [];
  const associatedLineItemIds: string[] = [];
  const uniqueLineItemIds = new Set<string>();
  const errorSamples: string[] = [];
  const dryRunPayloadSamples: Array<Record<string, string>> = [];
  const dryRunFingerprintSamples: string[] = [];

  let createdCount = 0;
  let dedupedCount = 0;
  let errorCount = 0;

  const pushError = (message: string): void => {
    errorCount += 1;
    if (errorSamples.length < 10) {
      errorSamples.push(message);
    }
  };

  const pushLineItemId = (lineItemId: string): void => {
    if (!uniqueLineItemIds.has(lineItemId)) {
      uniqueLineItemIds.add(lineItemId);
      associatedLineItemIds.push(lineItemId);
      if (lineItemIds.length < 50) {
        lineItemIds.push(lineItemId);
      }
    }
  };

  for (const charge of charges) {
    const chargeIdentifier =
      asNonEmptyString(charge.chargeId) ??
      asNonEmptyString(charge.id) ??
      asNonEmptyString(charge.chargeNumber);
    const orderChargeIdentifier = asNonEmptyString(charge.id);

    if (!chargeIdentifier) {
      pushError('Skipping charge with missing chargeId/id/chargeNumber for fingerprint');
      continue;
    }

    const fingerprint = `${row.subscription_id}:${row.term_end_date}:${chargeIdentifier}:${orderChargeIdentifier ?? 'no-order-charge-id'}`;
    const buildResult = buildLineItemProperties(
      charge,
      lineItemPropNames,
      fingerprint,
      forecastStartMs,
      forecastEndYmd,
      termMonths
    );

    if (!buildResult.ok) {
      pushError(buildResult.message);
      continue;
    }

    if (dryRun) {
      if (dryRunPayloadSamples.length < 5) {
        dryRunPayloadSamples.push(buildResult.properties);
      }
      if (dryRunFingerprintSamples.length < 5) {
        dryRunFingerprintSamples.push(buildResult.fingerprint);
      }
      continue;
    }

    try {
      const existingLineItemId = await searchLineItemByFingerprint(
        config.hubspotToken,
        lineItemPropNames.HS_LI_FINGERPRINT_PROP,
        buildResult.fingerprint
      );

      let lineItemId = existingLineItemId;
      if (lineItemId) {
        dedupedCount += 1;
      } else {
        lineItemId = await createHubSpotLineItem(config.hubspotToken, buildResult.properties);
        createdCount += 1;
      }

      await associateDealToLineItem(config.hubspotToken, createdDealId, lineItemId);
      pushLineItemId(lineItemId);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      pushError(`Charge ${buildResult.chargeIdentifier}: ${message}`);
    }
  }

  return {
    createdCount,
    dedupedCount,
    errorCount,
    lineItemIds,
    associatedLineItemIds,
    errorSamples,
    dryRunPayloadSamples,
    dryRunFingerprintSamples
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

  if (config.runMode !== 'test') {
    return jsonResponse(403, { error: "RUN_MODE must be 'test' to run this function" });
  }

  const providedSecret = getIngestSecretFromHeaders(req.headers);
  if (!providedSecret || providedSecret !== config.ingestSecret) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  let requestOptions: RequestOptions;
  try {
    requestOptions = await getRequestOptions(req);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(400, { error: message });
  }

  let lineItemPropNames: Record<string, string> | null = null;
  if (requestOptions.createLineItems) {
    const missingEnvVars = getMissingLineItemEnvVars(config);
    if (missingEnvVars.length > 0) {
      return jsonResponse(500, {
        error: `Missing required line item env vars: ${missingEnvVars.join(', ')}`
      });
    }

    lineItemPropNames = Object.fromEntries(
      REQUIRED_LINE_ITEM_PROPERTY_ENV_NAMES.map((name) => [name, config.lineItemPropertyEnv[name] as string])
    );
  }

  let supabase: ReturnType<typeof createClient> | null = null;
  let runId: string | null = null;
  let processedCount = 0;
  let created = 0;
  let errors = 0;
  let skippedLocked = 0;
  let results: RowResult[] = [];

  try {
    supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
      auth: { persistSession: false }
    });
    runId = await safeStartAutomationRun(supabase, {
      functionName: 'hubspot-renewal-create',
      triggerSource: requestOptions.sourceHubspotDealId ? 'webhook' : 'cron',
      runMode: config.runMode,
      sourceHubspotDealId: requestOptions.sourceHubspotDealId,
      metadata: {
        dry_run: requestOptions.dryRun,
        create_line_items: requestOptions.createLineItems,
        requested_limit: requestOptions.limit ?? 'all'
      }
    });

    let readyRowsQuery = supabase
      .from('renewals_ready_for_hubspot')
      .select('subscription_id,term_end_date,source_hubspot_deal_id,younium_charges_json')
      .order('term_end_date', { ascending: true });

    if (requestOptions.sourceHubspotDealId) {
      readyRowsQuery = readyRowsQuery.eq('source_hubspot_deal_id', requestOptions.sourceHubspotDealId);
    }

    if (requestOptions.limit != null) {
      readyRowsQuery = readyRowsQuery.limit(requestOptions.limit);
    }

    const { data: readyRowsData, error: readyRowsError } = await readyRowsQuery;

    if (readyRowsError) {
      throw new Error(`Failed to query renewals_ready_for_hubspot: ${readyRowsError.message}`);
    }

    const readyRows = (readyRowsData ?? []) as ReadyRow[];
    processedCount = readyRows.length;
    const companyDomainBySourceDeal = new Map<string, string | null>();

    for (const row of readyRows) {
      const resultBase = {
        subscription_id: row.subscription_id,
        term_end_date: row.term_end_date,
        source_hubspot_deal_id: row.source_hubspot_deal_id
      };

      let lineItemsCreatedCount = 0;
      let lineItemsDedupedCount = 0;
      let lineItemsErrorCount = 0;
      let lineItemIds: string[] = [];
      let lineItemErrorSamples: string[] = [];
      let rowClaimed = false;
      let createdDealIdForRow: string | null = null;
      let calculatedAmountForRow: string | null = null;

      try {
        const sourceDealUrl = new URL(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(row.source_hubspot_deal_id)}`);
        sourceDealUrl.searchParams.set('properties', SOURCE_DEAL_PROPERTIES.join(','));

        const sourceDealResponse = await fetch(sourceDealUrl.toString(), {
          method: 'GET',
          headers: getHubSpotHeaders(config.hubspotToken)
        });

        const sourceDealText = await sourceDealResponse.text();
        let sourceDealBody: unknown = null;

        try {
          sourceDealBody = sourceDealText ? JSON.parse(sourceDealText) : null;
        } catch {
          sourceDealBody = null;
        }

        if (!sourceDealResponse.ok) {
          const responseMessage = extractHubSpotMessage(sourceDealBody, sourceDealText || `HTTP ${sourceDealResponse.status}`);

          if (sourceDealResponse.status === 404) {
            const notFoundMessage = `Source deal ${row.source_hubspot_deal_id} not found (${responseMessage})`;
            if (!requestOptions.dryRun) {
              await markLedgerError(supabase, row, notFoundMessage);
            }
            errors += 1;
            results.push({
              ...resultBase,
              line_items_created_count: 0,
              line_items_deduped_count: 0,
              line_items_error_count: 0,
              line_item_ids: [],
              line_item_error_samples: [],
              error: notFoundMessage
            });
            continue;
          }

          errors += 1;
          results.push({
            ...resultBase,
            line_items_created_count: 0,
            line_items_deduped_count: 0,
            line_items_error_count: 0,
            line_item_ids: [],
            line_item_error_samples: [],
            error: `Failed to fetch source deal: ${responseMessage}`
          });
          continue;
        }

        const sourceProperties =
          isRecord(sourceDealBody) && isRecord(sourceDealBody.properties)
            ? (sourceDealBody.properties as SourceDealProperties)
            : {};

        const sourceDealId =
          isRecord(sourceDealBody)
            ? asNonEmptyString(sourceDealBody.id)
            : null;

        const { forecastStart, forecastEnd, termMonths } = calculateForecastDates(sourceProperties, row);
        const closedateMs = termEndDateToEpochMs(row.term_end_date);
        const forecastContractStartDate = formatUtcDateMMDDYYYY(forecastStart);

        let associatedCompanyDomain: string | null = null;
        if (companyDomainBySourceDeal.has(row.source_hubspot_deal_id)) {
          associatedCompanyDomain = companyDomainBySourceDeal.get(row.source_hubspot_deal_id) ?? null;
        } else {
          try {
            associatedCompanyDomain = await fetchSourceDealCompanyDomain(
              config.hubspotToken,
              row.source_hubspot_deal_id
            );
          } catch (domainError: unknown) {
            const domainMessage = domainError instanceof Error ? domainError.message : String(domainError);
            console.error(
              `Failed to resolve associated company domain for source deal ${row.source_hubspot_deal_id}: ${domainMessage}`
            );
            associatedCompanyDomain = null;
          }

          companyDomainBySourceDeal.set(row.source_hubspot_deal_id, associatedCompanyDomain);
        }

        const dealNameDomain =
          associatedCompanyDomain ??
          getPropertyString(sourceProperties, 'deal_company_domain') ??
          '{COMPANY DOMAIN}';
        const renewalDealName = `${dealNameDomain} Renewal ${forecastContractStartDate}`;
        const forecastStartMs = String(forecastStart.getTime());
        const forecastEndMs = String(forecastEnd.getTime());
        const forecastEndYmd = formatUtcDateYYYYMMDD(forecastEnd);

        const dealCreatePayload: Record<string, string> = {
          dealname: renewalDealName,
          pipeline: config.forecastPipelineId,
          dealstage: config.forecastDealstageId,
          closedate: closedateMs
        };

        // Owner assignment intentionally disabled during testing to avoid notifying reps.

        for (const field of DIRECT_COPY_FIELDS) {
          const value = getPropertyString(sourceProperties, field);
          if (value) {
            dealCreatePayload[field] = value;
          }
        }

        for (const field of SELECT_COPY_FIELDS) {
          const value = getPropertyString(sourceProperties, field);
          if (value) {
            dealCreatePayload[field] = value;
          }
        }

        for (const field of MULTISELECT_COPY_FIELDS) {
          const value = getPropertyString(sourceProperties, field);
          if (value) {
            dealCreatePayload[field] = value;
          }
        }

        if (associatedCompanyDomain) {
          dealCreatePayload.deal_company_domain = associatedCompanyDomain;
        }

        // Forecast overrides
        dealCreatePayload.dealtype = 'existingbusiness';
        dealCreatePayload.younium_change_type = 'change';
        dealCreatePayload.contract_start_date__c = forecastStartMs;
        dealCreatePayload.contract_end_date__c = forecastEndMs;
        dealCreatePayload.younium_order_effective_start_date = forecastStartMs;
        dealCreatePayload.younium_order_effective_change_date = forecastEndMs;
        dealCreatePayload.younium_initial_term = String(termMonths);
        dealCreatePayload.younium_renewal_term = String(termMonths);
        if (sourceDealId) {
          dealCreatePayload.original_deal_id = sourceDealId;
        }

        if (requestOptions.dryRun) {
          let lineItemSummary: LineItemRunSummary | null = null;

          if (requestOptions.createLineItems) {
            lineItemSummary = await processLineItemsForRow({
              config,
              lineItemPropNames: lineItemPropNames as Record<string, string>,
              row,
              createdDealId: 'dry-run',
              forecastStartMs,
              forecastEndYmd,
              termMonths,
              dryRun: true
            });

            lineItemsCreatedCount = lineItemSummary.createdCount;
            lineItemsDedupedCount = lineItemSummary.dedupedCount;
            lineItemsErrorCount = lineItemSummary.errorCount;
            lineItemIds = lineItemSummary.lineItemIds;
            lineItemErrorSamples = lineItemSummary.errorSamples;
          }

          results.push({
            ...resultBase,
            line_items_created_count: lineItemsCreatedCount,
            line_items_deduped_count: lineItemsDedupedCount,
            line_items_error_count: lineItemsErrorCount,
            line_item_ids: lineItemIds,
            line_item_error_samples: lineItemErrorSamples,
            would_create: {
              deal_payload: dealCreatePayload,
              ...(lineItemSummary
                ? {
                    line_item_property_payloads_sample: lineItemSummary.dryRunPayloadSamples,
                    fingerprint_examples: lineItemSummary.dryRunFingerprintSamples
                  }
                : {})
            }
          });
          continue;
        }

        const claimed = await claimLedgerForProcessing(supabase, row);
        if (!claimed) {
          skippedLocked += 1;
          results.push({
            ...resultBase,
            status_note: 'Skipped: renewal is already processing or already created by another run.',
            line_items_created_count: 0,
            line_items_deduped_count: 0,
            line_items_error_count: 0,
            line_item_ids: [],
            line_item_error_samples: []
          });
          continue;
        }
        rowClaimed = true;

        const createDealResponse = await fetch('https://api.hubapi.com/crm/v3/objects/deals', {
          method: 'POST',
          headers: {
            ...getHubSpotHeaders(config.hubspotToken),
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            properties: dealCreatePayload
          })
        });

        const createDealText = await createDealResponse.text();
        let createDealBody: unknown = null;

        try {
          createDealBody = createDealText ? JSON.parse(createDealText) : null;
        } catch {
          createDealBody = null;
        }

        if (!createDealResponse.ok) {
          const responseMessage = extractHubSpotMessage(createDealBody, createDealText || `HTTP ${createDealResponse.status}`);
          await releaseLedgerForRetry(supabase, row, `Failed to create forecast deal: ${responseMessage}`);
          errors += 1;
          results.push({
            ...resultBase,
            line_items_created_count: 0,
            line_items_deduped_count: 0,
            line_items_error_count: 0,
            line_item_ids: [],
            line_item_error_samples: [],
            error: `Failed to create forecast deal: ${responseMessage}`
          });
          continue;
        }

        const createdDealId =
          isRecord(createDealBody)
            ? asNonEmptyString(createDealBody.id)
            : null;

        if (!createdDealId) {
          await markLedgerError(supabase, row, 'HubSpot create deal response missing id');
          errors += 1;
          results.push({
            ...resultBase,
            line_items_created_count: 0,
            line_items_deduped_count: 0,
            line_items_error_count: 0,
            line_item_ids: [],
            line_item_error_samples: [],
            error: 'HubSpot create deal response missing id'
          });
          continue;
        }
        createdDealIdForRow = createdDealId;

        if (requestOptions.createLineItems) {
          const lineItemSummary = await processLineItemsForRow({
            config,
            lineItemPropNames: lineItemPropNames as Record<string, string>,
            row,
            createdDealId,
            forecastStartMs,
            forecastEndYmd,
            termMonths,
            dryRun: false
          });

          lineItemsCreatedCount = lineItemSummary.createdCount;
          lineItemsDedupedCount = lineItemSummary.dedupedCount;
          lineItemsErrorCount = lineItemSummary.errorCount;
          lineItemIds = lineItemSummary.lineItemIds;
          lineItemErrorSamples = lineItemSummary.errorSamples;

          if (lineItemSummary.associatedLineItemIds.length > 0) {
            try {
              const netTcv = await fetchLineItemsNetTcv(
                config.hubspotToken,
                lineItemSummary.associatedLineItemIds
              );
              await updateDealAmount(config.hubspotToken, createdDealId, netTcv);
              calculatedAmountForRow = toHubSpotAmountString(netTcv);
            } catch (amountError: unknown) {
              const amountMessage = amountError instanceof Error ? amountError.message : String(amountError);
              lineItemsErrorCount += 1;
              if (lineItemErrorSamples.length < 10) {
                lineItemErrorSamples.push(amountMessage);
              }
            }
          }
        }

        const nowIso = new Date().toISOString();
        const existingMetadata = await readLedgerMetadata(supabase, row);

        const mergedMetadata = {
          ...existingMetadata,
          source_hubspot_deal_id: row.source_hubspot_deal_id,
          created_deal_id: createdDealId,
          ...(calculatedAmountForRow ? { calculated_amount: calculatedAmountForRow } : {}),
          run_mode: config.runMode,
          timestamp: nowIso,
          ...(requestOptions.createLineItems
            ? {
                line_items_created_count: lineItemsCreatedCount,
                line_items_deduped_count: lineItemsDedupedCount,
                line_items_error_count: lineItemsErrorCount,
                line_item_ids: lineItemIds,
                last_line_item_run_at: nowIso
              }
            : {})
        };

        const { error: ledgerUpdateError } = await supabase
          .from('renewal_ledger')
          .update({
            hubspot_deal_id: createdDealId,
            status: 'created',
            metadata: mergedMetadata,
            updated_at: nowIso
          })
          .eq('subscription_id', row.subscription_id)
          .eq('term_end_date', row.term_end_date);

        if (ledgerUpdateError) {
          await markLedgerError(
            supabase,
            row,
            `Deal created but renewal_ledger update failed: ${ledgerUpdateError.message}`,
            { created_deal_id: createdDealId }
          );
          errors += 1;
          results.push({
            ...resultBase,
            created_deal_id: createdDealId,
            ...(calculatedAmountForRow ? { calculated_amount: calculatedAmountForRow } : {}),
            line_items_created_count: lineItemsCreatedCount,
            line_items_deduped_count: lineItemsDedupedCount,
            line_items_error_count: lineItemsErrorCount,
            line_item_ids: lineItemIds,
            line_item_error_samples: lineItemErrorSamples,
            error: `Deal created but renewal_ledger update failed: ${ledgerUpdateError.message}`
          });
          continue;
        }

        created += 1;
        results.push({
          ...resultBase,
          created_deal_id: createdDealId,
          ...(calculatedAmountForRow ? { calculated_amount: calculatedAmountForRow } : {}),
          line_items_created_count: lineItemsCreatedCount,
          line_items_deduped_count: lineItemsDedupedCount,
          line_items_error_count: lineItemsErrorCount,
          line_item_ids: lineItemIds,
          line_item_error_samples: lineItemErrorSamples
        });
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);

        if (!requestOptions.dryRun && rowClaimed) {
          try {
            if (createdDealIdForRow) {
              await markLedgerError(supabase, row, message, { created_deal_id: createdDealIdForRow });
            } else {
              await releaseLedgerForRetry(supabase, row, message);
            }
          } catch (stateError: unknown) {
            const stateMessage = stateError instanceof Error ? stateError.message : String(stateError);
            console.error(`Failed to update renewal_ledger state after row error: ${stateMessage}`);
          }
        }

        errors += 1;
        results.push({
          ...resultBase,
          ...(createdDealIdForRow ? { created_deal_id: createdDealIdForRow } : {}),
          ...(calculatedAmountForRow ? { calculated_amount: calculatedAmountForRow } : {}),
          line_items_created_count: lineItemsCreatedCount,
          line_items_deduped_count: lineItemsDedupedCount,
          line_items_error_count: lineItemsErrorCount,
          line_item_ids: lineItemIds,
          line_item_error_samples: lineItemErrorSamples,
          error: message
        });
      }
    }

    await safeInsertAutomationEvents(supabase, [
      {
        runId,
        functionName: 'hubspot-renewal-create',
        eventType: 'create_summary',
        status: errors > 0 ? 'partial' : 'success',
        sourceHubspotDealId: requestOptions.sourceHubspotDealId,
        detail: {
          dry_run: requestOptions.dryRun,
          create_line_items: requestOptions.createLineItems,
          requested_limit: requestOptions.limit ?? 'all',
          processed: processedCount,
          created,
          skipped_locked: skippedLocked,
          errors,
          error_samples: results
            .filter((row) => Boolean(row.error))
            .slice(0, 10)
            .map((row) => ({
              subscription_id: row.subscription_id,
              term_end_date: row.term_end_date,
              source_hubspot_deal_id: row.source_hubspot_deal_id,
              error: row.error
            }))
        }
      }
    ]);
    await safeFinishAutomationRun(supabase, {
      runId,
      status: errors > 0 ? 'partial' : 'success',
      httpStatus: 200,
      processedCount,
      createdCount: created,
      errorCount: errors,
      metadata: {
        dry_run: requestOptions.dryRun,
        create_line_items: requestOptions.createLineItems,
        requested_limit: requestOptions.limit ?? 'all',
        skipped_locked: skippedLocked,
        source_hubspot_deal_id: requestOptions.sourceHubspotDealId
      }
    });

    return jsonResponse(200, {
      requested_limit: requestOptions.limit ?? 'all',
      requested_source_hubspot_deal_id: requestOptions.sourceHubspotDealId,
      processed: processedCount,
      created,
      skipped_locked: skippedLocked,
      errors,
      results,
      timestamp: new Date().toISOString()
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);

    if (supabase) {
      await safeInsertAutomationEvents(supabase, [
        {
          runId,
          functionName: 'hubspot-renewal-create',
          eventType: 'run_failed',
          status: 'error',
          sourceHubspotDealId: requestOptions.sourceHubspotDealId,
          detail: {
            error: message,
            dry_run: requestOptions.dryRun,
            create_line_items: requestOptions.createLineItems,
            requested_limit: requestOptions.limit ?? 'all',
            processed: processedCount,
            created,
            skipped_locked: skippedLocked,
            errors
          }
        }
      ]);
      await safeFinishAutomationRun(supabase, {
        runId,
        status: 'error',
        httpStatus: 500,
        processedCount,
        createdCount: created,
        errorCount: Math.max(errors, 1),
        errorMessage: message,
        metadata: {
          dry_run: requestOptions.dryRun,
          create_line_items: requestOptions.createLineItems,
          requested_limit: requestOptions.limit ?? 'all',
          skipped_locked: skippedLocked,
          source_hubspot_deal_id: requestOptions.sourceHubspotDealId
        }
      });
    }

    return jsonResponse(500, { error: message });
  }
});
