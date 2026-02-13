/// <reference types="https://esm.sh/@supabase/functions-js/src/edge-runtime.d.ts" />

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

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
  limit: number;
  dryRun: boolean;
  createLineItems: boolean;
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

const DAY_IN_MS = 24 * 60 * 60 * 1000;

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

function dateLikeToEpochMsString(input: string, fieldName: string): string {
  const value = input.trim();
  let normalized = value;

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    normalized = `${value}T00:00:00.000Z`;
  } else if (/^\d{4}-\d{2}-\d{2}T/.test(value)) {
    const hasTimezone = /([zZ]|[+\-]\d{2}:\d{2})$/.test(value);
    if (!hasTimezone) {
      normalized = `${value}Z`;
    }
  }

  const ms = Date.parse(normalized);
  if (!Number.isFinite(ms)) {
    throw new Error(`Invalid ${fieldName}: ${input}`);
  }

  return String(ms);
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

function parseLimit(raw: unknown): number {
  if (raw == null) {
    return 5;
  }

  if (typeof raw !== 'number' || !Number.isFinite(raw)) {
    throw new Error('Invalid request body: "limit" must be a number');
  }

  if (raw <= 0) {
    throw new Error('Invalid request body: "limit" must be greater than 0');
  }

  return Math.min(Math.floor(raw), 5);
}

async function getRequestOptions(req: Request): Promise<RequestOptions> {
  const text = await req.text();

  if (!text.trim()) {
    return {
      limit: 5,
      dryRun: false,
      createLineItems: false
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

  return {
    limit: parseLimit(parsed.limit),
    dryRun: dryRunRaw === true,
    createLineItems: createLineItemsRaw === true
  };
}

function termEndDateToEpochMs(termEndDate: string): string {
  const ms = Date.parse(`${termEndDate}T00:00:00.000Z`);

  if (!Number.isFinite(ms)) {
    throw new Error(`Invalid term_end_date: ${termEndDate}`);
  }

  return String(ms);
}

function forecastStartDateFromTermEnd(termEndDate: string): string {
  const termEnd = parseIsoDateToUtcDate(termEndDate, 'term_end_date');
  const forecastStart = addUtcDays(termEnd, 1);
  return formatUtcDateMMDDYYYY(forecastStart);
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

  return chargeType === 'recurring' && changeState !== 'cancelled';
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

type ProductFrequencyLabel = 'Monthly' | 'Quarterly' | 'Annual' | 'Biannual';

type ProductFrequencyOverride = {
  name: string;
  hubspotId: string;
  frequency: ProductFrequencyLabel;
};

const PRODUCT_FREQUENCY_OVERRIDES: ProductFrequencyOverride[] = [
  { name: 'Signature Plan', hubspotId: '2324781', frequency: 'Monthly' },
  { name: 'Pipeline Plan', hubspotId: '3065596', frequency: 'Monthly' },
  { name: 'Complete Plan', hubspotId: '3563937', frequency: 'Monthly' },
  { name: 'Discount Recurring', hubspotId: '26391830', frequency: 'Annual' },
  { name: 'Discount One-Time', hubspotId: '27215512', frequency: 'Annual' },
  { name: 'Other (reccurring)', hubspotId: '27663040', frequency: 'Annual' },
  { name: 'Other (one-time)', hubspotId: '27659258', frequency: 'Annual' },
  { name: 'Monthly Billing Fee 20%', hubspotId: '38999861', frequency: 'Monthly' },
  { name: 'Quarterly Billing Fee 10%', hubspotId: '39001604', frequency: 'Quarterly' },
  { name: 'SPF Flattening - Tier 1', hubspotId: '1084489515', frequency: 'Annual' },
  { name: 'Additional Domains', hubspotId: '1084337011', frequency: 'Annual' },
  { name: 'Opensense Signature Package', hubspotId: '1483161702', frequency: 'Annual' },
  { name: 'Opensense Pipeline Package', hubspotId: '1483153265', frequency: 'Annual' },
  { name: 'Opensense Complete Package', hubspotId: '1483153267', frequency: 'Annual' },
  { name: 'Platform License', hubspotId: '2081778700', frequency: 'Monthly' },
  { name: 'Platform License - Premium', hubspotId: '2082106010', frequency: 'Monthly' },
  { name: 'Platform License - Enterprise', hubspotId: '2081778701', frequency: 'Monthly' },
  { name: 'Compliance Plan', hubspotId: '3379735458', frequency: 'Monthly' },
  { name: 'Digital Business Cards', hubspotId: '16031140980', frequency: 'Monthly' },
  { name: 'Signature Lite Plan', hubspotId: '20145631887', frequency: 'Monthly' },
  { name: 'Discount - Volume', hubspotId: '25082565921', frequency: 'Annual' },
  { name: 'Discount - Competitive', hubspotId: '25082194576', frequency: 'Annual' },
  { name: 'Discount - Platform License', hubspotId: '25082194577', frequency: 'Annual' },
  { name: 'Discount - Case Study', hubspotId: '25082565922', frequency: 'Annual' },
  { name: 'Discount - Social Proof', hubspotId: '25082194579', frequency: 'Annual' },
  { name: 'Discount Sales Incentives', hubspotId: '25082194581', frequency: 'Annual' },
  { name: 'Discount - Events', hubspotId: '25082194582', frequency: 'Annual' },
  { name: 'Discount - Waived Fees', hubspotId: '25082565924', frequency: 'Annual' },
  { name: 'Bronze Engagement Plan', hubspotId: '33136518206', frequency: 'Monthly' },
  { name: 'Silver Engagement Plan', hubspotId: '33136456258', frequency: 'Monthly' },
  { name: 'Gold Engagement Plan', hubspotId: '33136518231', frequency: 'Monthly' },
  { name: 'Signature Plan - GCC High', hubspotId: '40915323994', frequency: 'Monthly' },
  { name: 'Pipeline Plan - GCC High', hubspotId: '40912561672', frequency: 'Monthly' },
  { name: 'Complete Plan - GCC High', hubspotId: '40912561673', frequency: 'Monthly' },
  { name: 'Bronze Engagement Plan - GCC High', hubspotId: '40912375845', frequency: 'Monthly' },
  { name: 'Silver Engagement Plan - GCC High', hubspotId: '40912375846', frequency: 'Monthly' },
  { name: 'Gold Engagement Plan - GCC High', hubspotId: '40912499776', frequency: 'Monthly' }
];

function normalizeLookupKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function productFrequencyLabelToHubspot(value: ProductFrequencyLabel): string {
  if (value === 'Monthly') {
    return 'monthly';
  }
  if (value === 'Quarterly') {
    return 'quarterly';
  }
  if (value === 'Biannual') {
    return 'per_six_months';
  }
  return 'annually';
}

const PRODUCT_FREQUENCY_LOOKUP: Record<string, string> = (() => {
  const lookup: Record<string, string> = {};
  for (const item of PRODUCT_FREQUENCY_OVERRIDES) {
    const normalizedName = normalizeLookupKey(item.name);
    const normalizedId = normalizeLookupKey(item.hubspotId);
    const frequency = productFrequencyLabelToHubspot(item.frequency);

    lookup[normalizedName] = frequency;
    lookup[normalizedId] = frequency;
  }

  // Alias in case upstream spelling differs.
  lookup[normalizeLookupKey('Other (recurring)')] = 'annually';

  return lookup;
})();

function mapBillingPeriodToRecurringFrequency(value: unknown): string | null {
  const raw = asNonEmptyString(value);
  if (!raw) {
    return null;
  }

  const normalized = raw.toLowerCase().replace(/[\s_-]+/g, '');
  const mapping: Record<string, string> = {
    // Younium billingPeriod values
    annual: 'annually',
    monthly: 'monthly',
    quarterly: 'quarterly',
    biannual: 'per_six_months',
    endofterm: 'annually',
    // Label/value compatibility
    weekly: 'weekly',
    everytwoweeks: 'biweekly',
    semiannually: 'per_six_months',
    annually: 'annually',
    everytwoyears: 'per_two_years',
    everythreeyears: 'per_three_years',
    everyfouryears: 'per_four_years',
    everyfiveyears: 'per_five_years'
  };

  return mapping[normalized] ?? null;
}

function getNestedString(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  if (isRecord(value)) {
    return asNonEmptyString(value.id) ?? asNonEmptyString(value.name);
  }

  return asNonEmptyString(value);
}

function mapProductOverrideToRecurringFrequency(charge: Record<string, unknown>): string | null {
  const candidateValues = [
    asNonEmptyString(charge.name),
    asNonEmptyString(charge.productName),
    asNonEmptyString(charge.productId),
    asNonEmptyString(charge.hubspotProductId),
    getNestedString(charge, 'product')
  ].filter((value): value is string => Boolean(value));

  for (const candidate of candidateValues) {
    const normalized = normalizeLookupKey(candidate);
    const frequency = PRODUCT_FREQUENCY_LOOKUP[normalized];
    if (frequency) {
      return frequency;
    }
  }

  return null;
}

function mapChargeToRecurringFrequency(charge: Record<string, unknown>): string | null {
  const billingPeriodNormalized = normalizeLookupKey(asNonEmptyString(charge.billingPeriod) ?? '');

  // Override with billingPeriod for explicitly non-monthly/annual cadences.
  if (billingPeriodNormalized === 'quarterly') {
    return 'quarterly';
  }
  if (billingPeriodNormalized === 'biannual' || billingPeriodNormalized === 'semiannually') {
    return 'per_six_months';
  }

  const productOverride = mapProductOverrideToRecurringFrequency(charge);
  if (productOverride) {
    return productOverride;
  }

  return mapBillingPeriodToRecurringFrequency(charge.billingPeriod);
}

function buildLineItemProperties(
  charge: Record<string, unknown>,
  row: ReadyRow,
  lineItemPropNames: Record<string, string>,
  fingerprint: string
): ChargeBuildResult {
  const chargeId = asNonEmptyString(charge.chargeId) ?? asNonEmptyString(charge.id);
  const orderChargeId = asNonEmptyString(charge.id);
  const chargeNumber = asNonEmptyString(charge.chargeNumber);
  const chargeName = asNonEmptyString(charge.name) ?? chargeNumber ?? chargeId ?? 'Younium Charge';
  const rawQuantity = asFiniteNumber(charge.quantity) ?? 1;
  const quantity = rawQuantity > 0 ? rawQuantity : 1;
  const unitPrice = findUnitPrice(charge);
  const effectiveStartDate = asNonEmptyString(charge.effectiveStartDate);
  const effectiveEndDate = asNonEmptyString(charge.effectiveEndDate);
  const recurringBillingFrequency = mapChargeToRecurringFrequency(charge);

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

  if (!effectiveStartDate) {
    return {
      ok: false,
      message: `Missing effectiveStartDate for charge ${chargeId}`
    };
  }

  if (!recurringBillingFrequency) {
    const billingPeriod = asNonEmptyString(charge.billingPeriod) ?? '<missing>';
    return {
      ok: false,
      message: `Unsupported billingPeriod "${billingPeriod}" for charge ${chargeId}`
    };
  }

  const sourceStartDate = parseDateLikeToUtcDate(
    effectiveStartDate,
    `effectiveStartDate for charge ${chargeId}`
  );
  const forecastStartDate = addUtcDays(parseIsoDateToUtcDate(row.term_end_date, 'term_end_date'), 1);

  let forecastEndDate: Date;
  if (effectiveEndDate) {
    const sourceEndDate = parseDateLikeToUtcDate(
      effectiveEndDate,
      `effectiveEndDate for charge ${chargeId}`
    );

    if (sourceEndDate.getTime() < sourceStartDate.getTime()) {
      return {
        ok: false,
        message: `effectiveEndDate is before effectiveStartDate for charge ${chargeId}`
      };
    }

    const durationDays = Math.floor((sourceEndDate.getTime() - sourceStartDate.getTime()) / DAY_IN_MS) + 1;
    forecastEndDate = addUtcDays(forecastStartDate, Math.max(durationDays - 1, 0));
  } else {
    const annualEnd = new Date(
      Date.UTC(
        forecastStartDate.getUTCFullYear() + 1,
        forecastStartDate.getUTCMonth(),
        forecastStartDate.getUTCDate()
      )
    );
    forecastEndDate = addUtcDays(annualEnd, -1);
  }

  const effectiveStartDateMs = String(forecastStartDate.getTime());
  const forecastEndDateYmd = formatUtcDateYYYYMMDD(forecastEndDate);
  const unitPricePerQuantity = unitPrice / quantity;

  const normalizedUnitPrice = unitPricePerQuantity < 0 ? 0 : unitPricePerQuantity;
  const unitDiscount = unitPricePerQuantity < 0 ? Math.abs(unitPricePerQuantity) : null;

  const properties: Record<string, string> = {
    name: chargeName,
    quantity: String(quantity),
    price: String(normalizedUnitPrice),
    recurringbillingfrequency: recurringBillingFrequency,
    [lineItemPropNames.HS_LI_YOUNIUM_CHARGE_EFFECTIVE_START_DATE_PROP]: effectiveStartDateMs,
    [lineItemPropNames.HS_LI_YOUNIUM_CHARGE_EFFECTIVE_END_DATE_PROP]: forecastEndDateYmd,
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

async function processLineItemsForRow(params: {
  config: RuntimeConfig;
  lineItemPropNames: Record<string, string>;
  row: ReadyRow;
  createdDealId: string;
  dryRun: boolean;
}): Promise<LineItemRunSummary> {
  const { config, lineItemPropNames, row, createdDealId, dryRun } = params;

  const allCharges = parseCharges(row.younium_charges_json);
  const charges = allCharges.filter(isRecurringAndNotCancelled);

  const lineItemIds: string[] = [];
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

    if (!chargeIdentifier) {
      pushError('Skipping charge with missing chargeId/id/chargeNumber for fingerprint');
      continue;
    }

    const fingerprint = `${row.subscription_id}:${row.term_end_date}:${chargeIdentifier}`;
    const buildResult = buildLineItemProperties(charge, row, lineItemPropNames, fingerprint);

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

  const providedSecret = req.headers.get('x-ingest-secret');
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

  try {
    const supabase = createClient(config.supabaseUrl, config.supabaseServiceRoleKey, {
      auth: { persistSession: false }
    });

    const { data: readyRowsData, error: readyRowsError } = await supabase
      .from('renewals_ready_for_hubspot')
      .select('subscription_id,term_end_date,source_hubspot_deal_id,younium_charges_json')
      .order('term_end_date', { ascending: true })
      .limit(requestOptions.limit);

    if (readyRowsError) {
      throw new Error(`Failed to query renewals_ready_for_hubspot: ${readyRowsError.message}`);
    }

    const readyRows = ((readyRowsData ?? []) as ReadyRow[]).slice(0, requestOptions.limit);

    let created = 0;
    let errors = 0;
    const results: RowResult[] = [];

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

      try {
        const sourceDealUrl = new URL(`https://api.hubapi.com/crm/v3/objects/deals/${encodeURIComponent(row.source_hubspot_deal_id)}`);
        sourceDealUrl.searchParams.set(
          'properties',
          'dealname,dealstage,pipeline,amount,closedate,hubspot_owner_id'
        );

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

        const sourceOwnerId = asNonEmptyString(sourceProperties.hubspot_owner_id);

        const closedateMs = termEndDateToEpochMs(row.term_end_date);
        const forecastContractStartDate = forecastStartDateFromTermEnd(row.term_end_date);
        const renewalDealName = `{COMPANY DOMAIN} Renewal ${forecastContractStartDate}`;

        const dealCreatePayload: Record<string, string> = {
          dealname: renewalDealName,
          pipeline: config.forecastPipelineId,
          dealstage: config.forecastDealstageId,
          closedate: closedateMs
        };

        if (sourceOwnerId) {
          dealCreatePayload.hubspot_owner_id = sourceOwnerId;
        }

        if (requestOptions.dryRun) {
          let lineItemSummary: LineItemRunSummary | null = null;

          if (requestOptions.createLineItems) {
            lineItemSummary = await processLineItemsForRow({
              config,
              lineItemPropNames: lineItemPropNames as Record<string, string>,
              row,
              createdDealId: 'dry-run',
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

        if (requestOptions.createLineItems) {
          const lineItemSummary = await processLineItemsForRow({
            config,
            lineItemPropNames: lineItemPropNames as Record<string, string>,
            row,
            createdDealId,
            dryRun: false
          });

          lineItemsCreatedCount = lineItemSummary.createdCount;
          lineItemsDedupedCount = lineItemSummary.dedupedCount;
          lineItemsErrorCount = lineItemSummary.errorCount;
          lineItemIds = lineItemSummary.lineItemIds;
          lineItemErrorSamples = lineItemSummary.errorSamples;
        }

        const nowIso = new Date().toISOString();
        const existingMetadata = await readLedgerMetadata(supabase, row);

        const mergedMetadata = {
          ...existingMetadata,
          source_hubspot_deal_id: row.source_hubspot_deal_id,
          created_deal_id: createdDealId,
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
          errors += 1;
          results.push({
            ...resultBase,
            created_deal_id: createdDealId,
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
          line_items_created_count: lineItemsCreatedCount,
          line_items_deduped_count: lineItemsDedupedCount,
          line_items_error_count: lineItemsErrorCount,
          line_item_ids: lineItemIds,
          line_item_error_samples: lineItemErrorSamples
        });
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        errors += 1;
        results.push({
          ...resultBase,
          line_items_created_count: lineItemsCreatedCount,
          line_items_deduped_count: lineItemsDedupedCount,
          line_items_error_count: lineItemsErrorCount,
          line_item_ids: lineItemIds,
          line_item_error_samples: lineItemErrorSamples,
          error: message
        });
      }
    }

    return jsonResponse(200, {
      requested_limit: requestOptions.limit,
      processed: readyRows.length,
      created,
      errors,
      results,
      timestamp: new Date().toISOString()
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, { error: message });
  }
});
