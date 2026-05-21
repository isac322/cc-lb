# Real Client Versions

Pinned versions are installed by `tests/real-client/install.sh` with `npm install --prefix target/test-bins/<client> <pkg>@<exact-version>`.

| Client | npm package | Binary | Pinned version | Confirmation command | Output |
| --- | --- | --- | --- | --- | --- |
| `claude-code` | `@anthropic-ai/claude-code` | `claude` | `2.1.146` | `npm view @anthropic-ai/claude-code version` | `2.1.146` |
| `opencode` | `opencode-ai` | `opencode` | `1.15.6` | `npm view opencode-ai version` | `1.15.6` |
| `pi` | `@earendil-works/pi-coding-agent` | `pi` | `0.75.4` | `npm view @earendil-works/pi-coding-agent version` | `0.75.4` |

`@earendil-works/pi-ai` was also publicly resolvable at `0.75.4`, but the executable client harness pins `@earendil-works/pi-coding-agent` because it exposes the `pi` binary.
