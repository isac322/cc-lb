import { useState, useEffect } from 'react';
import { getJson, ConfigSchemaResponse } from '../api';
import { MOCK_SCHEMA } from './mockData';

export function useConfigSchema() {
  const [schema, setSchema] = useState<ConfigSchemaResponse | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const isMock = new URLSearchParams(window.location.search).get('mock') === '1';
    if (isMock) {
      setSchema(MOCK_SCHEMA);
      setLoading(false);
      return;
    }

    getJson<ConfigSchemaResponse>('/admin/config/schema')
      .then((data) => {
        setSchema(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err instanceof Error ? err : new Error(String(err)));
        setLoading(false);
      });
  }, []);

  return { schema, error, loading };
}
