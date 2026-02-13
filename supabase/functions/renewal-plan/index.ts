/// <reference types="https://esm.sh/@supabase/functions-js/src/edge-runtime.d.ts" />

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

type RenewalCandidate = {
  subscription_id: string;
  term_end_date: string;
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

Deno.serve(async (req: Request): Promise<Response> => {
  if (req.method !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed. Use POST.' });
  }

  const providedSecret = req.headers.get('x-ingest-secret');
  const expectedSecret = Deno.env.get('INGEST_SECRET');

  if (!providedSecret || !expectedSecret || providedSecret !== expectedSecret) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  try {
    const supabaseUrl = getRequiredEnv('SUPABASE_URL');
    const serviceRoleKey = getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY');

    const supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false }
    });

    const { data: candidates, error: candidatesError } = await supabase
      .from('renewal_candidates_90d_unprocessed')
      .select('subscription_id,term_end_date');

    if (candidatesError) {
      throw new Error(`Failed to query renewal candidates: ${candidatesError.message}`);
    }

    const candidateRows = (candidates ?? []) as RenewalCandidate[];
    const candidatesFound = candidateRows.length;
    const timestamp = new Date().toISOString();

    if (candidatesFound === 0) {
      return jsonResponse(200, {
        candidates_found: 0,
        planned_inserted: 0,
        timestamp
      });
    }

    const uniqueCandidates = new Map<string, RenewalCandidate>();
    for (const candidate of candidateRows) {
      if (!candidate.subscription_id || !candidate.term_end_date) {
        continue;
      }

      const key = `${candidate.subscription_id}::${candidate.term_end_date}`;
      uniqueCandidates.set(key, candidate);
    }

    const ledgerRows = Array.from(uniqueCandidates.values()).map((candidate) => ({
      subscription_id: candidate.subscription_id,
      term_end_date: candidate.term_end_date,
      status: 'planned',
      created_source: 'auto'
    }));

    if (ledgerRows.length === 0) {
      return jsonResponse(200, {
        candidates_found: candidatesFound,
        planned_inserted: 0,
        timestamp
      });
    }

    const { data: insertedRows, error: upsertError } = await supabase
      .from('renewal_ledger')
      .upsert(ledgerRows, {
        onConflict: 'subscription_id,term_end_date',
        ignoreDuplicates: true
      })
      .select('subscription_id,term_end_date');

    if (upsertError) {
      throw new Error(`Failed to write renewal ledger: ${upsertError.message}`);
    }

    return jsonResponse(200, {
      candidates_found: candidatesFound,
      planned_inserted: (insertedRows ?? []).length,
      timestamp
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return jsonResponse(500, { error: message });
  }
});
