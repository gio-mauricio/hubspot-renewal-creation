import { env } from './lib/env.js';
import { pool, upsertSubscription, type YouniumSubscription as DbSubscription } from './lib/db.js';
import { fetchSubscriptionsPage, getAccessToken, type YouniumSubscription as ApiSubscription } from './lib/younium.js';

function isActiveSubscription(subscription: ApiSubscription): subscription is ApiSubscription & { id: string; status: 'Active' } {
  return subscription.status === 'Active' && typeof subscription.id === 'string' && subscription.id.length > 0;
}

function toDbSubscription(subscription: ApiSubscription): DbSubscription {
  return subscription as DbSubscription;
}

async function main(): Promise<void> {
  console.log('Starting Younium subscriptions ingestion...');
  console.log(`Base URL: ${env.YOUNIUM_BASE_URL}`);
  console.log(`Page size: ${env.PAGE_SIZE}`);

  const token = await getAccessToken();

  let pageNumber = 1;
  let totalUpserts = 0;

  while (true) {
    let subscriptions: ApiSubscription[];
    try {
      subscriptions = await fetchSubscriptionsPage(token, env.PAGE_SIZE, pageNumber);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`Failed to fetch page ${pageNumber}: ${message}`);
      throw error;
    }

    const fetchedCount = subscriptions.length;

    if (fetchedCount === 0) {
      break;
    }

    const activeSubscriptions = subscriptions.filter(isActiveSubscription);

    for (const subscription of activeSubscriptions) {
      await upsertSubscription(toDbSubscription(subscription));
      totalUpserts += 1;
    }

    console.log(
      `Page ${pageNumber}: fetched=${fetchedCount}, active=${activeSubscriptions.length}, total_upserts=${totalUpserts}`
    );

    pageNumber += 1;
  }

  console.log(`Ingestion complete. Total upserts: ${totalUpserts}`);
}

main()
  .catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Ingestion failed: ${message}`);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
