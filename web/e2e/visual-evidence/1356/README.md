# PR #1356 visual evidence

Captured from PR head `7648cdb4b12073c8c0ef2a371447ec607c105799` against the real SQLite admin API with Vertex upstreams created by the real-credential E2E run. The project id was redacted to `example-project` in the DOM before every screenshot.

Capture script: an out-of-tree Playwright script. Browser: Chromium. Height: 900px.

| Surface | 390px | 768px | 1440px |
|---|---|---|---|
| Upstreams list | `after-upstreams-list-390.png` | `after-upstreams-list-768.png` | `after-upstreams-list-1440.png` |
| Google Cloud connection options | `after-connect-google-cloud-390.png` | `after-connect-google-cloud-768.png` | `after-connect-google-cloud-1440.png` |
| Credential-file form | `after-credential-file-form-390.png` | `after-credential-file-form-768.png` | `after-credential-file-form-1440.png` |
| Credential section | `after-credential-section-390.png` | `after-credential-section-768.png` | `after-credential-section-1440.png` |
| Express detail | `after-express-detail-390.png` | `after-express-detail-768.png` | `after-express-detail-1440.png` |

All 15 captures passed the horizontal-overflow check (`scrollWidth === innerWidth`). The empty credential form is write-only and shows no credential contents. Every PNG was inspected; no secrets or unredacted project id appear.
