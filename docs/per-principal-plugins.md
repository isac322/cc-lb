# Per-Principal Plugin Overrides

This feature allows you to configure custom router and observability plugins for individual principals.

## Configuration Syntax

You can configure plugins on a per-principal basis using the `router_plugin` and `observability_hooks` fields under each principal in your configuration file.

### Inheritance Semantics

- **Inherit Global Default**: Omit the field entirely. The principal inherits the global default plugin chain.
- **Explicit Empty**: Set `observability_hooks = []`. This overrides the global default, ensuring no hooks run for this principal.

### TOML Example

Here is a configuration showing global defaults and two principals with overrides:

```toml
[plugins]
# Global defaults
router_plugin = { name = "global-router", path = "plugins/global_router.wasm" }
observability_hooks = [
    { name = "global-logger", path = "plugins/global_logger.wasm" }
]

[principals.alice]
api_key = "sk-alice-key"
# Alice uses a custom router plugin but inherits the global logger hook
router_plugin = { name = "alice-router", path = "plugins/alice_router.wasm" }

[principals.bob]
api_key = "sk-bob-key"
# Bob inherits the global router but explicitly disables all observability hooks
observability_hooks = []
```

## Admin Status Endpoint

The `/admin/status` endpoint returns active plugin information. Active plugin details are returned in a JSON response that retains the legacy `plugins` array and adds a `principals` map.

### Response Shape

```json
{
  "plugins": [
    { "name": "global-router", "config_hash": "a1b2c3d4e5f6g7h8" }
  ],
  "principals": {
    "alice": {
      "router_plugin": { "name": "alice-router", "config_hash": "8h7g6f5e4d3c2b1a" },
      "observability_hooks": null
    },
    "bob": {
      "router_plugin": null,
      "observability_hooks": []
    }
  }
}
```

The `config_hash` is a SHA-256 hex prefix (first 16 characters) of the plugin configuration, keeping sensitive parameters redacted.

## Failure Modes and Hot-Reload

The configuration reload process uses all-or-nothing validation. If any `PluginRef` fails to load or validate during a hot-reload (triggered by SIGHUP):

1. The reload aborts.
2. Active traffic continues using the previous configuration and plugin views.
3. Errors are reported via the `last_reload_status` field on `/admin/status`.
