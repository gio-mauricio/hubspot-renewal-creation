import { Pool } from 'pg';
import { env } from './env.js';

export type YouniumSubscription = {
  id?: string;
  accountNumber?: string;
  account?: unknown;
  status?: string;
  effectiveStartDate?: string | null;
  effectiveEndDate?: string | null;
  cancellationDate?: string | null;
  [key: string]: unknown;
};

export const pool = new Pool({
  connectionString: env.DATABASE_URL
});

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

      const fromItem = asNonEmptyString((item as Record<string, unknown>).accountNumber);
      if (fromItem) {
        return fromItem;
      }
    }

    return null;
  }

  if (account && typeof account === 'object') {
    return asNonEmptyString((account as Record<string, unknown>).accountNumber);
  }

  return null;
}

export async function upsertSubscription(subscription: YouniumSubscription): Promise<void> {
  if (!subscription.id) {
    throw new Error('Cannot upsert subscription without id');
  }

  const accountNumber = resolveAccountNumber(subscription);
  if (!accountNumber) {
    throw new Error(`Cannot upsert subscription ${subscription.id}: missing accountNumber`);
  }

  const query = `
    INSERT INTO public.younium_subscriptions (
      subscription_id,
      account_number,
      status,
      effective_start_date,
      effective_end_date,
      cancellation_date,
      raw_json,
      updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, NOW())
    ON CONFLICT (subscription_id)
    DO UPDATE SET
      account_number = EXCLUDED.account_number,
      status = EXCLUDED.status,
      effective_start_date = EXCLUDED.effective_start_date,
      effective_end_date = EXCLUDED.effective_end_date,
      cancellation_date = EXCLUDED.cancellation_date,
      raw_json = EXCLUDED.raw_json,
      updated_at = NOW()
  `;

  const values = [
    subscription.id,
    accountNumber,
    subscription.status ?? null,
    subscription.effectiveStartDate ?? null,
    subscription.effectiveEndDate ?? null,
    subscription.cancellationDate ?? null,
    JSON.stringify(subscription)
  ];

  await pool.query(query, values);
}
