const API_BASE = '/api/v1';

interface RequestOptions {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
}

class ApiClient {
  private token: string | null = null;

  setToken(token: string | null) {
    this.token = token;
  }

  authorizationHeaders(): Record<string, string> {
    return this.token ? { Authorization: `Bearer ${this.token}` } : {};
  }

  async fetch<T>(path: string, options: RequestOptions = {}): Promise<T> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...options.headers,
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    const response = await fetch(`${API_BASE}${path}`, {
      method: options.method ?? 'GET',
      headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
    });

    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: response.statusText }));
      throw new ApiError(response.status, errorMessage(payload, response.statusText || 'Unknown error'));
    }

    if (response.status === 204) return undefined as T;
    return response.json();
  }

  get<T>(path: string) {
    return this.fetch<T>(path);
  }

  post<T>(path: string, body: unknown) {
    return this.fetch<T>(path, { method: 'POST', body });
  }

  put<T>(path: string, body: unknown) {
    return this.fetch<T>(path, { method: 'PUT', body });
  }

  patch<T>(path: string, body: unknown) {
    return this.fetch<T>(path, { method: 'PATCH', body });
  }

  delete<T>(path: string) {
    return this.fetch<T>(path, { method: 'DELETE' });
  }
}

/** The message of an error the API answered: the service's own string, the
 *  message inside the gateway's `error` envelope, or the fallback. */
export function errorMessage(payload: unknown, fallback: string): string {
  const raw = payload && typeof payload === 'object'
    ? (payload as { error?: unknown }).error ?? (payload as { message?: unknown }).message
    : null;
  if (typeof raw === 'string') return raw;
  if (raw && typeof raw === 'object' && typeof (raw as { message?: unknown }).message === 'string') {
    return (raw as { message: string }).message;
  }
  return fallback;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export const api = new ApiClient();
export default api;
