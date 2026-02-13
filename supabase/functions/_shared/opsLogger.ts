import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

type SupabaseClient = ReturnType<typeof createClient>;

type TriggerSource = 'cron' | 'manual' | 'webhook';
type RunStatus = 'running' | 'success' | 'partial' | 'error';

type StartRunInput = {
  functionName: string;
  triggerSource: TriggerSource;
  runMode?: string | null;
  sourceHubspotDealId?: string | null;
  metadata?: Record<string, unknown>;
};

type FinishRunInput = {
  runId: string | null;
  status: Exclude<RunStatus, 'running'>;
  httpStatus: number;
  processedCount?: number;
  createdCount?: number;
  errorCount?: number;
  errorMessage?: string | null;
  metadata?: Record<string, unknown>;
};

export type AutomationEventInput = {
  runId?: string | null;
  functionName: string;
  eventType: string;
  subscriptionId?: string | null;
  termEndDate?: string | null;
  sourceHubspotDealId?: string | null;
  status?: string | null;
  detail?: Record<string, unknown>;
};

function asInt(value: number | undefined): number {
  if (value == null || !Number.isFinite(value)) {
    return 0;
  }

  return Math.max(0, Math.trunc(value));
}

async function startAutomationRun(
  supabase: SupabaseClient,
  input: StartRunInput
): Promise<string | null> {
  const { data, error } = await supabase
    .from('automation_runs')
    .insert({
      function_name: input.functionName,
      trigger_source: input.triggerSource,
      run_mode: input.runMode ?? null,
      source_hubspot_deal_id: input.sourceHubspotDealId ?? null,
      status: 'running',
      metadata: input.metadata ?? {}
    })
    .select('id')
    .maybeSingle();

  if (error) {
    throw new Error(`automation_runs start insert failed: ${error.message}`);
  }

  return (data?.id as string | undefined) ?? null;
}

async function finishAutomationRun(
  supabase: SupabaseClient,
  input: FinishRunInput
): Promise<void> {
  if (!input.runId) {
    return;
  }

  const { error } = await supabase
    .from('automation_runs')
    .update({
      status: input.status,
      http_status: input.httpStatus,
      processed_count: asInt(input.processedCount),
      created_count: asInt(input.createdCount),
      error_count: asInt(input.errorCount),
      error_message: input.errorMessage ?? null,
      metadata: input.metadata ?? {},
      finished_at: new Date().toISOString()
    })
    .eq('id', input.runId);

  if (error) {
    throw new Error(`automation_runs finish update failed: ${error.message}`);
  }
}

async function insertAutomationEvents(
  supabase: SupabaseClient,
  events: AutomationEventInput[]
): Promise<void> {
  if (!events.length) {
    return;
  }

  const rows = events.map((event) => ({
    run_id: event.runId ?? null,
    function_name: event.functionName,
    event_type: event.eventType,
    subscription_id: event.subscriptionId ?? null,
    term_end_date: event.termEndDate ?? null,
    source_hubspot_deal_id: event.sourceHubspotDealId ?? null,
    status: event.status ?? null,
    detail: event.detail ?? {}
  }));

  const { error } = await supabase.from('automation_events').insert(rows);
  if (error) {
    throw new Error(`automation_events insert failed: ${error.message}`);
  }
}

export async function safeStartAutomationRun(
  supabase: SupabaseClient,
  input: StartRunInput
): Promise<string | null> {
  try {
    return await startAutomationRun(supabase, input);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[opsLogger] ${message}`);
    return null;
  }
}

export async function safeFinishAutomationRun(
  supabase: SupabaseClient,
  input: FinishRunInput
): Promise<void> {
  try {
    await finishAutomationRun(supabase, input);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[opsLogger] ${message}`);
  }
}

export async function safeInsertAutomationEvents(
  supabase: SupabaseClient,
  events: AutomationEventInput[]
): Promise<void> {
  try {
    await insertAutomationEvents(supabase, events);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[opsLogger] ${message}`);
  }
}

