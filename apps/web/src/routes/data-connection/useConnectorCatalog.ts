import { useQuery } from '@tanstack/react-query';

import { dataConnection, FALLBACK_CONNECTOR_CATALOG, type Source } from '@/lib/api/data-connection';

// Static descriptions are usable offline. Snapshot sync depends on the server's
// installed runtime and, for S3, the operator's endpoint configuration.
const offlineCatalog = FALLBACK_CONNECTOR_CATALOG.map((entry) => ({
  ...entry,
  capabilities: entry.capabilities.filter((capability) => capability !== 'batch_sync'),
}));

export function useConnectorCatalog() {
  const query = useQuery({ queryKey: ['data-connection', 'catalog'], queryFn: dataConnection.getCatalog });
  return query.isError ? offlineCatalog : (query.data?.connectors ?? offlineCatalog);
}

export function useSourceCatalogEntry(source: Source | null | undefined) {
  const catalog = useConnectorCatalog();
  const capabilities = useQuery({
    queryKey: ['data-connection', 'source-capabilities', source?.id],
    queryFn: () => dataConnection.getSourceCapabilities(source!.id),
    enabled: Boolean(source),
  });
  const entry = catalog.find((item) => item.type === source?.connector_type);
  if (!entry) return undefined;
  const effective = capabilities.isError ? undefined : capabilities.data?.capabilities;
  return {
    ...entry,
    capabilities: entry.capabilities.filter((item) => {
      if (item === 'batch_sync') return effective?.native_snapshot_sync;
      if (item === 'virtual_table') return effective?.supports_zero_copy;
      if (item === 'cdc_sync') return effective?.supports_cdc;
      if (item === 'exploration') return effective?.supports_discovery;
      return true;
    }),
  };
}
