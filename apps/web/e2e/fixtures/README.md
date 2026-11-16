# e2e fixtures

## `orders-real.xlsx` (7158 bytes)

Real OpenXML workbook for the P3.1 Excel-import real-backend suite
(`dataset-excel-real.spec.ts`). Produced by the backend worker's isolated
Excelize **2.11.1** generator; byte-identical copy of the handoff fixture at
`/private/tmp/openfoundry-p31-review/fixtures/orders.xlsx`.

- sha256: `1d52beb7a870225116357011720553462f991095a67a2e3122159f4c512d745f`
  (matches `runtime-public.json`)
- Sheets: `说明` (notes, first sheet), `订单` (header at physical row 3; values
  include leading-zero `007`, 30-digit codes, rich-text cell, blank cell),
  `原始` (no header; 2 content rows × 2 columns).

Other files here (`tiny-workbook.xlsx`, `second-book.xlsx`, `legacy-book.xls`,
`cities.csv`) are tiny placeholders for the mocked-HTTP UI suite — their bytes
are never parsed; only names/extensions matter.

## Real-backend suite: opt-in and environment

`dataset-excel-real.spec.ts` talks to a **real** p31 stack. It is skipped in
default Playwright runs; enable explicitly:

```sh
P31_REAL_E2E=1 pnpm exec playwright test --config=playwright.p31-real.config.ts
```

| Variable | Default (loopback) | Purpose |
|---|---|---|
| `P31_REAL_E2E` | unset → suite **skipped** | `1` enables the real suite |
| `P31_TOKEN_FILE` | `/private/tmp/openfoundry-p31-review/token_admin.txt` | Admin JWT read **at runtime only**; never hardcode the value |
| `P31_FIXTURE` | `e2e/fixtures/orders-real.xlsx` | Workbook under test (repo copy by default) |
| `P31_WEB_PORT` | `5195` | Test-only Vite dev port (identity stub + `/api` proxy) |
| `P31_GW_URL` | `http://127.0.0.1:3931` | Edge-gateway base the dev server proxies `/api` to |

Requirements to run it for real: the isolated p31 containers (gw 3931 / dvs
3932) up, and the token file present with `dataset.write` claims. The
identity endpoints (`/api/v1/auth/bootstrap-status`, `/api/v1/users/me`) are
stubbed locally by `vite.p31-e2e.config.ts` with the stable admin-fixture
profile documented in the handoff `runtime-public.json` (sub/org/roles) — the
isolated stack has no auth service. Everything else is real gateway traffic.

The mocked-HTTP UI suite (`dataset-excel-import.spec.ts`,
`playwright.excel.config.ts`, port 5194) needs no external services.
