/// <reference types="https://esm.sh/@supabase/functions-js/src/edge-runtime.d.ts" />

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';
import {
  safeFinishAutomationRun,
  safeInsertAutomationEvents,
  safeStartAutomationRun
} from '../_shared/opsLogger.ts';

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

function getIngestSecretFromHeaders(headers: Headers): string | null {
  return headers.get('x-ingest-secret') ?? headers.get('x_ingest_secret');
}

Deno.serve(async (req: Request): Promise<Response> => {
  if (req.method !== 'POST') {
    return jsonResponse(405, { error: 'Method not allowed. Use POST.' });
  }

  const providedSecret = getIngestSecretFromHeaders(req.headers);
  const expectedSecret = Deno.env.get('INGEST_SECRET');

  if (!providedSecret || !expectedSecret || providedSecret !== expectedSecret) {
    return jsonResponse(401, { error: 'Unauthorized' });
  }

  let supabase: ReturnType<typeof createClient> | null = null;
  let runId: string | null = null;
  let candidatesFound = 0;
  let plannedInserted = 0;

  try {
    const supabaseUrl = getRequiredEnv('SUPABASE_URL');
    const serviceRoleKey = getRequiredEnv('SUPABASE_SERVICE_ROLE_KEY');

    supabase = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false }
    });
    runId = await safeStartAutomationRun(supabase, {
      functionName: 'renewal-plan',
      triggerSource: 'cron'
    });

    const { data: candidates, error: candidatesError } = await supabase
      .from('renewal_candidates_90d_unprocessed')
      .select('subscription_id,term_end_date');

    if (candidatesError) {
      throw new Error(`Failed to query renewal candidates: ${candidatesError.message}`);
    }

    const candidateRows = (candidates ?? []) as RenewalCandidate[];
    candidatesFound = candidateRows.length;
    const timestamp = new Date().toISOString();

    if (candidatesFound === 0) {
      await safeInsertAutomationEvents(supabase, [
        {
          runId,
          functionName: 'renewal-plan',
          eventType: 'no_candidates',
          status: 'success',
          detail: { candidates_found: 0 }
        }
      ]);
      await safeFinishAutomationRun(supabase, {
        runId,
        status: 'success',
        httpStatus: 200,
        processedCount: 0,
        createdCount: 0,
        errorCount: 0
      });
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
      await safeInsertAutomationEvents(supabase, [
        {
          runId,
          functionName: 'renewal-plan',
          eventType: 'no_valid_candidates',
          status: 'success',
          detail: { candidates_found: candidatesFound }
        }
      ]);
      await safeFinishAutomationRun(supabase, {
        runId,
        status: 'success',
        httpStatus: 200,
        processedCount: candidatesFound,
        createdCount: 0,
        errorCount: 0
      });
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
    plannedInserted = (insertedRows ?? []).length;

    await safeInsertAutomationEvents(supabase, [
      {
        runId,
        functionName: 'renewal-plan',
        eventType: 'planned_upsert_summary',
        status: 'success',
        detail: {
          candidates_found: candidatesFound,
          planned_inserted: plannedInserted
        }
      }
    ]);
    await safeFinishAutomationRun(supabase, {
      runId,
      status: 'success',
      httpStatus: 200,
      processedCount: candidatesFound,
      createdCount: plannedInserted,
      errorCount: 0
    });

    return jsonResponse(200, {
      candidates_found: candidatesFound,
      planned_inserted: plannedInserted,
      timestamp
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);

    if (supabase) {
      await safeInsertAutomationEvents(supabase, [
        {
          runId,
          functionName: 'renewal-plan',
          eventType: 'run_failed',
          status: 'error',
          detail: {
            error: message,
            candidates_found: candidatesFound,
            planned_inserted: plannedInserted
          }
        }
      ]);
      await safeFinishAutomationRun(supabase, {
        runId,
        status: 'error',
        httpStatus: 500,
        processedCount: candidatesFound,
        createdCount: plannedInserted,
        errorCount: 1,
        errorMessage: message
      });
    }

    return jsonResponse(500, { error: message });
  }
});
