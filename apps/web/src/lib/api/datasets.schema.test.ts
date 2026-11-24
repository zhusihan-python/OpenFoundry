import { describe, expect, it } from 'vitest';

import { ApiError } from './client';
import { schemaFormatUnsupported } from './datasets';

describe('schemaFormatUnsupported', () => {
  it('recognizes only the format limitation of the versioned schema', () => {
    expect(schemaFormatUnsupported(
      new ApiError(422, 'dataset_schema_format_unsupported: versioned schemas require Parquet ADD files'),
    )).toBe(true);
    // An unreadable snapshot is not a snapshot without a schema to diff.
    expect(schemaFormatUnsupported(
      new ApiError(422, 'dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path'),
    )).toBe(false);
    expect(schemaFormatUnsupported(new Error('dataset_schema_format_unsupported'))).toBe(false);
    expect(schemaFormatUnsupported(null)).toBe(false);
  });
});
