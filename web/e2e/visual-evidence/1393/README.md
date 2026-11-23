# PR #1393 visual evidence

Captured from PR head `cfaa5427533e32ff06b8ade46217227c0d5ca2d0` against the built Admin Web SPA served via Vite preview (`bun run preview`) with sanitized Playwright route fixtures.

## Captures

| Route / View | Viewport | Theme | File | Assertions |
| --- | --- | --- | --- | --- |
| Overview | 1440x900 | Dark | [after-overview-1440-dark.png](./after-overview-1440-dark.png) | No horizontal overflow (`scrollWidth === innerWidth`), zero console/page/HTTP errors |
| Overview | 1440x900 | Light | [after-overview-1440-light.png](./after-overview-1440-light.png) | No horizontal overflow, light palette rendered cleanly |
| Overview | 390x900 | Dark | [after-overview-390-dark.png](./after-overview-390-dark.png) | Mobile drawer/header responsive, `scrollWidth === innerWidth` |
| Upstreams | 1440x900 | Dark | [after-upstreams-1440-dark.png](./after-upstreams-1440-dark.png) | Upstream list, status badges, and quota bars render cleanly |
| Principals | 1440x900 | Dark | [after-principals-1440-dark.png](./after-principals-1440-dark.png) | Cache keepalive live metrics & inherited OpenAI API compat policy render cleanly |
| Logs & Events | 1440x900 | Dark | [after-logs-1440-dark.png](./after-logs-1440-dark.png) | Event log table and detail view render cleanly |
| Plugins | 1440x900 | Dark | [after-plugins-1440-dark.png](./after-plugins-1440-dark.png) | Plugin inventory and health cards render cleanly |
| Settings | 1440x900 | Dark | [after-settings-1440-dark.png](./after-settings-1440-dark.png) | Config editor sections and controls render cleanly |
| Audit | 1440x900 | Dark | [after-audit-1440-dark.png](./after-audit-1440-dark.png) | Audit trail table renders cleanly |
