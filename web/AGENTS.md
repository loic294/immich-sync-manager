# ISM web: rules for coding agents

Same stack and conventions as Open Photo Manager's web UI, so both admin UIs look and behave alike:
Lit 3 + DaisyUI 5 / Tailwind 4, Vite 8, TypeScript 7 (strict), Biome, Vitest + happy-dom, signals
(`@preact/signals-core`), routing `@lit-labs/router`, icons `lucide`.
Work from `web/`. `src/api/generated/` is written by the Rust build (ts-rs): never edit it.

## Commands
- `npm run dev`: Vite on :5173, proxying `/api` to `ISM_API_TARGET` (default `http://127.0.0.1:8080`).
- `npm run format`: Biome auto-fix. Run it before the gate; `npm run lint` only reports.
- `npm run check` = typecheck + lint + test + build. It must be green before you finish.

## UI
- DaisyUI components and tokens only; layout with Tailwind utilities; no custom CSS or hex colours.
- Write complete class names (no `btn-${kind}`): Tailwind drops concatenated names.
- Light DOM, decorator-free Lit: `static override properties` plus `declare`; initialise state in the
  constructor. Never change `useDefineForClassFields` (a test guards it).
- One component per file; tag = file name (`ism-foo.ts` defines `<ism-foo>`); test beside it.

## Pages
- Every admin page extends `IsmPage` (`src/base/ism-page.ts`): implement `load()`, set
  `reloadOn` to the SSE change types that should refresh it, render through `frame()`, and run
  mutations through `act(key, fn, successMessage)` so busy state, toasts and 401 handling are shared.
- The shell (`src/app/ism-app.ts`) owns the `<h1>`; pages use `<h2>`. Add pages to
  `src/app/routes.ts`.
- Endpoints live in `src/api/admin.ts`. File changes (restore, purge, moves) go through
  `admin.submit()` as managed operations, never through ad-hoc calls.
- Destructive actions ask first with `confirmAction()`.

## Tests and privacy
- Mock `api.get`/`api.post` with `vi.spyOn`; mount with `mount()` from `src/test/helpers.ts`.
- Fixtures use synthetic names and `*.example` hosts only. Never commit real paths, hosts or people.
