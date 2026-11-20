# Real-client SDK checks

## OpenAI OAuth public compatibility

Run these checks against an **already running** `cc-lb` whose eligible OpenAI
upstreams are real `openai_oauth` accounts. The scripts do not create or delete
upstreams, principals, or keys and exit non-zero when a required case fails.
They use the OpenAI SDK installed in the invoking Node/Python environment.

```bash
export CC_LB_PROXY_URL=https://127.0.0.1:18951
export CC_LB_API_KEY='cclb_...'
export CC_LB_MODEL=gpt-6.2-sol       # optional; this is the default
export CC_LB_CA_CERT=/path/to/ca.pem # optional

node tests/real-client/provider-e2e/openai-oauth-public-compat.mjs
python3 tests/real-client/provider-e2e/openai_oauth_public_compat.py
```

The scripts cover public Responses, Chat Completions, models, compaction,
input-token counting, image generation/editing, WebSocket lanes, and the
transcription-disabled contract. To also verify the live per-principal
`store_true: reject` override and automatic restoration, set
`CC_LB_ADMIN_URL`, `CC_LB_ADMIN_TOKEN`, and `CC_LB_PRINCIPAL_ID`. Without
those optional variables that one case is reported as `SKIP`.
