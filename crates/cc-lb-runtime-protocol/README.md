# cc-lb-runtime-protocol

This crate implements the core host-side plugin protocol for cc-lb. It handles the low-level mechanics of loading, negotiating, and executing WebAssembly plugins using Extism.

By separating the protocol logic from the main server runtime, this crate provides a clean boundary that avoids coupling with storage, database, or network layers.

## Purpose

The primary purpose of this crate is to define and execute the five-layer plugin protocol used by the cc-lb host:

1. **Identity**: Reading the plugin's metadata and custom sections.
2. **Handshake**: Negotiating supported versions and capabilities between the host and the plugin.
3. **Self-Check**: Running the plugin's internal self-diagnostic routines.
4. **Persistence**: (Handled by the host registry, not included here).
5. **Dispatch**: Executing the plugin's handlers with proper wire-format envelopes.

This crate is used internally by the cc-lb host runtime and serves as the foundation for the plugin conformance testing suite.

## Public Surface

The crate exposes the following modules:

* `identity`: Functions to read and validate plugin metadata.
* `handshake`: Version negotiation and handshake execution.
* `self_check`: Diagnostic execution and report parsing.
* `dispatch`: Low-level wire call dispatching.
* `host_functions`: Host-provided functions for plugins, such as state storage and quota management.

## Compile Cost Warning

This crate depends directly on Extism and transitively on Wasmtime. Because of this, compiling this crate is resource-intensive and takes a noticeable amount of time. We recommend using it only where necessary, such as in host runtimes or test suites.

## SemVer Note

This crate tracks the cc-lb host version 1:1. Any changes to the underlying plugin protocol or host-side requirements will result in a corresponding version bump.
