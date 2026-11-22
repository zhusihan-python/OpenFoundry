import { useAuth } from '@/lib/stores/auth';

// This controls Pipeline Builder UI affordances. API authorization is server-owned.
export function usePipelineAuthoringAccess() {
  const { user, loading } = useAuth();
  const permissions = user?.permissions ?? [];
  const canEdit = permissions.includes('pipeline.write')
    || permissions.includes('pipeline:write')
    || permissions.includes('*');
  return { canEdit, loading };
}
