import { env } from './env.js';

export type YouniumSubscription = {
  id?: string;
  status?: string;
  [key: string]: unknown;
};

type TokenResponse = {
  access_token?: string;
  accessToken?: string;
};

function parseSubscriptionsPayload(payload: unknown): YouniumSubscription[] {
  if (Array.isArray(payload)) {
    return payload as YouniumSubscription[];
  }

  if (payload && typeof payload === 'object') {
    const candidate = payload as Record<string, unknown>;

    if (Array.isArray(candidate.items)) {
      return candidate.items as YouniumSubscription[];
    }

    if (Array.isArray(candidate.data)) {
      return candidate.data as YouniumSubscription[];
    }

    if (Array.isArray(candidate.value)) {
      return candidate.value as YouniumSubscription[];
    }
  }

  return [];
}

export async function getAccessToken(): Promise<string> {
  const url = `${env.YOUNIUM_BASE_URL}/auth/v2/token`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify({
      clientId: env.YOUNIUM_CLIENT_ID,
      secret: env.YOUNIUM_SECRET
    })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Token request failed (${response.status}): ${text}`);
  }

  const text = await response.text();
  let body: TokenResponse;
  try {
    body = JSON.parse(text) as TokenResponse;
  } catch {
    throw new Error(`Failed to parse token response JSON: ${text}`);
  }
  const token = body.access_token ?? body.accessToken;

  if (!token) {
    throw new Error('Token response did not include access_token or accessToken');
  }

  return token;
}

export async function fetchSubscriptionsPage(
  token: string,
  pageSize: number,
  pageNumber: number
): Promise<YouniumSubscription[]> {
  const url = new URL(`${env.YOUNIUM_BASE_URL}/Subscriptions`);
  url.searchParams.set('PageSize', String(pageSize));
  url.searchParams.set('PageNumber', String(pageNumber));

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json',
      'api-version': env.YOUNIUM_API_VERSION,
      'legal-entity': env.YOUNIUM_LEGAL_ENTITY
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
