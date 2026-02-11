import dotenv from 'dotenv';

dotenv.config();

function getRequired(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getOptional(name: string, defaultValue: string): string {
  const value = process.env[name]?.trim();
  return value || defaultValue;
}

function parsePositiveInt(name: string, raw: string): number {
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Environment variable ${name} must be a positive integer. Received: ${raw}`);
  }
  return parsed;
}

export const env = {
  DATABASE_URL: getRequired('DATABASE_URL'),
  YOUNIUM_BASE_URL: getRequired('YOUNIUM_BASE_URL').replace(/\/$/, ''),
  YOUNIUM_CLIENT_ID: getRequired('YOUNIUM_CLIENT_ID'),
  YOUNIUM_SECRET: getRequired('YOUNIUM_SECRET'),
  YOUNIUM_LEGAL_ENTITY: getRequired('YOUNIUM_LEGAL_ENTITY'),
  YOUNIUM_API_VERSION: getOptional('YOUNIUM_API_VERSION', '2.1'),
  PAGE_SIZE: parsePositiveInt('PAGE_SIZE', getOptional('PAGE_SIZE', '200'))
} as const;
