declare module 'pg' {
  export class Pool {
    constructor(config?: { connectionString?: string });
    query(text: string, values?: unknown[]): Promise<{ rows: unknown[] }>;
    end(): Promise<void>;
  }
}
