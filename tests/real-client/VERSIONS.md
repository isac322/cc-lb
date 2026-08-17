# Real Client Versions

Pinned versions are installed by `tests/real-client/install.sh` under `target/test-bins/<client>`. The script prefers `bun add` and falls back to `npm install`, except for `pi`, which uses npm with exact companion-package pins so the tested binary and companion packages stay in lockstep.

| Client | npm package | Binary | Pinned version | Confirmation command | Output |
| --- | --- | --- | --- | --- | --- |
| `claude-code` | `@anthropic-ai/claude-code` | `claude` | `2.1.233` | `npm view @anthropic-ai/claude-code version` | `2.1.233` |
| `opencode` | `opencode-ai` | `opencode` | `1.18.18` | `npm view opencode-ai version` | `1.18.18` |
| `pi` | `@earendil-works/pi-coding-agent` | `pi` | `0.84.2` | `npm view @earendil-works/pi-coding-agent version` | `0.84.2` |
| `senpi` | `@code-yeongyu/senpi` | `senpi` | `2026.8.16` | `npm view @code-yeongyu/senpi@2026.8.16 version` | `2026.8.16` |

The `pi` install pins `@earendil-works/pi-ai`, `@earendil-works/pi-agent-core`, and `@earendil-works/pi-tui` to `0.84.2`; `@earendil-works/pi-coding-agent` is the package that exposes the tested `pi` binary.
