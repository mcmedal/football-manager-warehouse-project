import sql from 'mssql';

const config: sql.config = {
  user: process.env.MSSQL_USER || 'sa',
  password: process.env.MSSQL_PASSWORD || 'McMedal@14',
  server: process.env.MSSQL_SERVER || 'localhost',
  port: parseInt(process.env.MSSQL_PORT || '1433', 10),
  database: process.env.MSSQL_DATABASE || 'DW',
  options: {
    encrypt: false,
    trustServerCertificate: true,
    enableArithAbort: true,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

declare global {
  // eslint-disable-next-line no-var
  var _mssqlPool: sql.ConnectionPool | undefined;
}

export async function getDbPool(): Promise<sql.ConnectionPool> {
  if (!global._mssqlPool || !global._mssqlPool.connected) {
    global._mssqlPool = await new sql.ConnectionPool(config).connect();
  }
  return global._mssqlPool;
}

export async function executeQuery<T = Record<string, unknown>>(
  queryText: string,
  params: Record<string, unknown> = {}
): Promise<T[]> {
  const pool = await getDbPool();
  const request = pool.request();
  for (const [key, val] of Object.entries(params)) {
    request.input(key, val);
  }
  const result = await request.query(queryText);
  return result.recordset as T[];
}
