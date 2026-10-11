Optional cache-safe subscription HTTP 429 failover is default-off through
`prompt_cache_shadow.subscription_failover`. It makes one distinct OAuth attempt
on `/v1/messages`, preserves configured filters and provider affinity, and shares
process-local full-TTL conversation admission with keepalive. See
[operator limits](../../docs/cache-safe-subscription-failover.md).
