import {
  CompiledQuery,
  DatabaseConnection,
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  DEFAULT_MIGRATION_LOCK_TABLE,
  DEFAULT_MIGRATION_TABLE,
  Dialect,
  Driver,
  Kysely,
  QueryCompiler,
  QueryResult,
  SchemaMetadata,
  SqliteAdapter,
  SqliteQueryCompiler,
  TableMetadata,
} from 'kysely';
import type { D1Database } from '@cloudflare/workers-types';

/**
 * Config for the D1 dialect. Pass your D1 instance to this object that you bound in `wrangler.toml`.
 */
export interface D1DialectConfig {
  database: D1Database;
}

/**
 * D1 dialect that adds support for [Cloudflare D1][0] in [Kysely][1].
 * The constructor takes the instance of your D1 database that you bound in `wrangler.toml`.
 *
 * ```typescript
 * new D1Dialect({
 *   database: env.DB,
 * })
 * ```
 *
 * [0]: https://blog.cloudflare.com/introducing-d1/
 * [1]: https://github.com/koskimas/kysely
 */
export class D1Dialect implements Dialect {
  #config: D1DialectConfig;

  constructor(config: D1DialectConfig) {
    this.#config = config;
  }

  createAdapter() {
    return new SqliteAdapter();
  }

  createDriver(): Driver {
    return new D1Driver(this.#config);
  }

  createQueryCompiler(): QueryCompiler {
    return new SqliteQueryCompiler();
  }

  createIntrospector(_db: Kysely<any>): DatabaseIntrospector {
    return new D1Introspector(this.#config.database);
  }
}

class D1Introspector implements DatabaseIntrospector {
  #d1: D1Database;

  constructor(d1: D1Database) {
    this.#d1 = d1;
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    return [];
  }

  async getTables(options: DatabaseMetadataOptions = { withInternalKyselyTables: false }): Promise<TableMetadata[]> {
    // Filter D1's internal `_cf_*` tables (e.g. `_cf_KV`, `_cf_METADATA`); these
    // exist in `sqlite_master` but are not user tables and break naive consumers
    // (e.g. better-auth's migrate command).
    let query = "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'";
    const params: string[] = [];
    if (!options.withInternalKyselyTables) {
      query += ' AND name != ? AND name != ?';
      params.push(DEFAULT_MIGRATION_TABLE, DEFAULT_MIGRATION_LOCK_TABLE);
    }
    query += ' ORDER BY name';

    const tablesResult = await this.#d1
      .prepare(query)
      .bind(...params)
      .all<{ name: string; sql: string | null }>();
    const tables = tablesResult.results ?? [];

    if (tables.length === 0) return [];

    // Fetch column metadata in a single batched round-trip rather than
    // Kysely's default `Promise.all` fan-out, which can swamp D1 on schemas
    // with many tables.
    const stmts = tables.map((t) => this.#d1.prepare('SELECT * FROM pragma_table_info(?)').bind(t.name));
    const batchResults = await this.#d1.batch<{
      cid: number;
      name: string;
      type: string;
      notnull: number;
      dflt_value: string | null;
      pk: number;
    }>(stmts);

    return tables.map((table, i) => {
      const cols = batchResults[i]?.results ?? [];

      let autoIncrementCol = table.sql
        ?.split(/[(),]/)
        ?.find((s) => s.toLowerCase().includes('autoincrement'))
        ?.trimStart()
        ?.split(/\s+/)?.[0]
        ?.replace(/["`]/g, '');

      // `INTEGER PRIMARY KEY` is an implicit rowid alias and auto-increments
      // even without the explicit AUTOINCREMENT keyword.
      if (!autoIncrementCol) {
        const pkCols = cols.filter((c) => c.pk > 0);
        if (pkCols.length === 1 && pkCols[0].type.toLowerCase() === 'integer') {
          autoIncrementCol = pkCols[0].name;
        }
      }

      return {
        name: table.name,
        columns: cols.map((c) => ({
          name: c.name,
          dataType: c.type,
          isNullable: !c.notnull,
          isAutoIncrementing: c.name === autoIncrementCol,
          hasDefaultValue: c.dflt_value != null,
        })),
      };
    });
  }

  async getMetadata(options?: DatabaseMetadataOptions): Promise<DatabaseMetadata> {
    return { tables: await this.getTables(options) };
  }
}

class D1Driver implements Driver {
  #config: D1DialectConfig;

  constructor(config: D1DialectConfig) {
    this.#config = config;
  }

  async init(): Promise<void> {}

  async acquireConnection(): Promise<DatabaseConnection> {
    return new D1Connection(this.#config);
  }

  async beginTransaction(conn: D1Connection): Promise<void> {
    return await conn.beginTransaction();
  }

  async commitTransaction(conn: D1Connection): Promise<void> {
    return await conn.commitTransaction();
  }

  async rollbackTransaction(conn: D1Connection): Promise<void> {
    return await conn.rollbackTransaction();
  }

  async releaseConnection(_conn: D1Connection): Promise<void> {}

  async destroy(): Promise<void> {}
}

class D1Connection implements DatabaseConnection {
  #config: D1DialectConfig;
  //   #transactionClient?: D1Connection

  constructor(config: D1DialectConfig) {
    this.#config = config;
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    // Transactions are not supported yet.
    // if (this.#transactionClient) return this.#transactionClient.executeQuery(compiledQuery)

    const results = await this.#config.database
      .prepare(compiledQuery.sql)
      .bind(...compiledQuery.parameters)
      .all();
    if (results.error) {
      throw new Error(results.error);
    }

    const numAffectedRows = results.meta.changes > 0 ? BigInt(results.meta.changes) : undefined;

    return {
      insertId:
        results.meta.last_row_id === undefined || results.meta.last_row_id === null
          ? undefined
          : BigInt(results.meta.last_row_id),
      rows: (results?.results as O[]) || [],
      numAffectedRows, // requires kysely >= 0.23
    };
  }

  async beginTransaction() {
    // this.#transactionClient = this.#transactionClient ?? new PlanetScaleConnection(this.#config)
    // this.#transactionClient.#conn.execute('BEGIN')
    throw new Error('Transactions are not supported yet.');
  }

  async commitTransaction() {
    // if (!this.#transactionClient) throw new Error('No transaction to commit')
    // this.#transactionClient.#conn.execute('COMMIT')
    // this.#transactionClient = undefined
    throw new Error('Transactions are not supported yet.');
  }

  async rollbackTransaction() {
    // if (!this.#transactionClient) throw new Error('No transaction to rollback')
    // this.#transactionClient.#conn.execute('ROLLBACK')
    // this.#transactionClient = undefined
    throw new Error('Transactions are not supported yet.');
  }

  async *streamQuery<O>(_compiledQuery: CompiledQuery, _chunkSize: number): AsyncIterableIterator<QueryResult<O>> {
    throw new Error('D1 Driver does not support streaming');
  }
}
