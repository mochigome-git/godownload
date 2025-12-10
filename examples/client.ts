// Example TypeScript client for the download service

interface TableConfig {
  table: string;
  filters?: Record<string, any>;
  select?: string;
  order_by?: Array<{ column: string; ascending: boolean }>;
}

interface DownloadRequest {
  tables: TableConfig[];
  roll_numbers: number[];
  is_five_min?: boolean;
  batch_size?: number;
  concurrency?: number;
}

interface TableData {
  table: string;
  data: any[];
  count: number;
}

interface DownloadResponse {
  tables: TableData[];
}

/**
 * Download data from multiple Supabase tables through the Lambda service
 *
 * @example
 * // Simple usage with table names
 * const result = await downloadData(
 *   supabaseToken,
 *   [
 *     { table: 'sensor_data' },
 *     { table: 'device_logs', filters: { status: 'active' } }
 *   ],
 *   [1, 2, 3, 4, 5]
 * );
 *
 * @example
 * // Advanced usage with all options
 * const result = await downloadData(
 *   supabaseToken,
 *   [
 *     {
 *       table: 'readings',
 *       filters: { device_id: 'abc123' },
 *       select: 'id,value,created_at',
 *       order_by: [{ column: 'created_at', ascending: true }]
 *     }
 *   ],
 *   rollNumbers,
 *   { isFiveMin: true, batchSize: 50, concurrency: 10 }
 * );
 */
export async function downloadData(
  supabaseToken: string,
  tableConfigs: (string | TableConfig)[],
  rollNumbers: number[],
  options?: {
    isFiveMin?: boolean;
    batchSize?: number;
    concurrency?: number;
    apiEndpoint?: string;
  }
): Promise<DownloadResponse> {
  const apiEndpoint =
    options?.apiEndpoint || process.env.DOWNLOAD_API_ENDPOINT || "";

  // Normalize table configs
  const tables: TableConfig[] = tableConfigs.map((config) =>
    typeof config === "string" ? { table: config } : config
  );

  const request: DownloadRequest = {
    tables,
    roll_numbers: rollNumbers,
    is_five_min: options?.isFiveMin,
    batch_size: options?.batchSize,
    concurrency: options?.concurrency,
  };

  const response = await fetch(`${apiEndpoint}/download`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${supabaseToken}`,
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ error: "Unknown error" }));
    throw new Error(error.error || `HTTP ${response.status}`);
  }

  return response.json();
}

// ----- Example usage equivalent to your original code -----

import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_ANON_KEY!
);

async function fetchDataWithLambda(
  tableConfigs: (string | { table: string; filters?: Record<string, any> })[],
  rollNumbers: number[],
  isFiveMin: boolean = false
) {
  // Get the user's JWT token
  const {
    data: { session },
  } = await supabase.auth.getSession();
  if (!session) {
    throw new Error("Not authenticated");
  }

  // Call the Lambda service
  const result = await downloadData(
    session.access_token,
    tableConfigs.map((config) =>
      typeof config === "string"
        ? { table: config }
        : { table: config.table, filters: config.filters }
    ),
    rollNumbers,
    {
      isFiveMin,
      batchSize: 20, // Same as your BATCH_SIZE
      concurrency: 5, // Same as your CONCURRENCY
    }
  );

  return result.tables;
}

// Usage example matching your original pattern:
/*
const tableData = await fetchDataWithLambda(
  [
    'table1',
    { table: 'table2', filters: { device_id: 'xyz' } },
    { table: 'table3', filters: { status: 'active', type: 'sensor' } }
  ],
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  true  // isFiveMin
);

// Response format:
// [
//   { table: 'table1', data: [...], count: 100 },
//   { table: 'table2', data: [...], count: 50 },
//   { table: 'table3', data: [...], count: 75 }
// ]
*/
