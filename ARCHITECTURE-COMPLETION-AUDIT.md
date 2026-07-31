# Grok Build Architecture Completion Audit

Audit date: 2026-07-31

Checkout: `/Users/mac/Documents/grok-build`

Branch: `codex/local-runtime-observability`

## Verdict

The architecture described in the attached control-plane plan is **not fully completed**. Its implemented control-plane foundation is now captured in commit `c5e60e1` (`Add local control plane and provider protocol`).

The checkout contains a substantial, compiling foundation, but the production execution path is not yet routed through `grokd`. The protocol, control-plane daemon, bounded engagement writer, MCP connector, and capability manifests are committed. The permission/approval work and audit-ledger-dependent replay changes remain deliberately excluded from that commit.

Consolidation state:

- Foundation code commit: `c5e60e1`
- Previous feature tip: `c20ca85`
- Previous `weaponized/main`: `958e5dc`
- Consolidation strategy: fast-forward both weaponized branches to the final scoped feature tip
- Excluded local edits remain preserved in the original checkout
- Repository visibility: public

The upstream `origin/main` mirror is a separate remote and is intentionally not folded into this weaponized-branch consolidation.

### Compact C2/team-client foundation added after the audit

The following architectural seams are now implemented without adding them to the headless deployment dependency graph:

- Stable team/client correlation identity carried from the client handshake into accepted engagement events and projections. This is not a permission model.
- Bounded, durable event cursor reads with optional 30-second long polling, reconnect replay, engagement filtering, lag recovery from the journal, and a bounded recent-event cache.
- First-class `InteractiveSession`, `DeferredTask`, and `DetachedJob` dispatch receipts.
- Typed runtime profiles covering protocol compatibility, deployment/libc targets, resource ceilings, provider requirements, immutable artifact hashes, and separate CLI/daemon/GUI size budgets.
- Offline JSON profile linting in `grokctl` and validated profile activation in `grokd` before durable state is opened.
- A thin Bash-compatible `grokctl` client with JSON responses and JSONL event output.
- A separate optional `grok-ui` executable using `egui`, `winit`, and the `glow` renderer. GUI dependencies are mechanically excluded from `grokctl` and `grokd`; `wgpu` is excluded from the active GUI graph.
- Static-musl deployment targets and a stripped `release-small` profile, with dependency and binary-size gates.

Measured Apple Silicon `release-small` artifacts:

- `grokctl`: 670,480 bytes.
- `grokd`: 2,378,128 bytes.
- `grok-ui`: 4,095,024 bytes.

The x86_64 musl client was cross-linked with Rust's bundled `rust-lld` as a stripped 1,000,016-byte static PIE. A complete musl `grokd` link requires a musl cross C compiler for bundled SQLite on the build host; it does not require that compiler or glibc on the deployed host.

## Missing work, sorted by importance

### P0 — Required before this is an operational control plane

#### 1. Make `grokd` the real owner of execution

`grokd` currently opens the control-plane state and Unix socket, but it does not construct or own:

- `RuntimeManager`
- `TurnCoordinator`
- `ExecutionSupervisor`
- Active memory retrieval and consolidation
- Embedding/indexing workers
- Operational MCP providers

Production callers still use process-wide globals:

- `RuntimeManager::global()` remains in `xai-grok-runtime` and is used by `xai-grok-sampler`.
- `ExecutionSupervisor::global()` remains in `xai-grok-tools` and is used directly by terminal and Nmap paths.
- `ResourceGovernor::global()` and the global execution-job registry also remain active.

Until those paths are injected from or proxied through `grokd`, the daemon is an additional state owner rather than the single state owner.

#### 2. Bootstrap executable providers and add a provider transport

`grokd` starts with an empty `ProviderRegistry`. It registers no LiteRT, native execution, memory, embedding, MCP, or indexing provider during startup.

The socket-level `RegisterProvider` command records a capability manifest in `ServiceSupervisor`, but it does not attach an executable provider implementation or a worker transport. Dispatch can only reach providers inserted through the in-process `ControlPlaneHandle::register_provider` method. The standalone daemon never calls that method.

Required work:

- Define the worker/provider binary transport.
- Bind registration to an executable connection, process, or local provider handle.
- Bootstrap LiteRT and native execution providers in `grokd`.
- Register memory, embedding, indexing, and MCP provider classes.
- Fence provider connections and in-flight work by service generation.
- Reject metadata-only providers as non-dispatchable.

Without this, the daemon can accept plans and manifests but cannot execute a real task.

#### 3. Implement the durable scheduler

`SubmitPlan` validates and stores a tasking plan. It does not automatically:

- Select dependency-ready tasks.
- Resolve providers.
- Apply interactive/deferred priority.
- Dispatch tasks.
- Advance dependents after observations.
- Evaluate completion tests.
- Recover an interrupted task graph after restart.

Dispatch currently requires a separate explicit `Dispatch` command. The target `Objective -> ExecutionTask -> ProviderDispatch -> Observation -> EvidenceRecord -> CompletionDecision` lifecycle is therefore represented as types but is not a running scheduler.

#### 4. Route the cursor event stream into the existing TUI

The Unix socket now exposes bounded durable cursor reads and long polling. Reconnecting shell and GUI clients replay from their last sequence, and slow consumers recover from the journal without creating an unbounded queue.

`ControlPlaneHandle::subscribe()` is available only to code running inside the daemon process. The pager and shell do not depend on `xai-grok-control-plane` and do not use `ControlPlaneClient`.

Required work:

- Add the thin control client to the existing TUI.
- Persist the TUI cursor and reconnect through the implemented replay contract.
- Remove TUI reads from runtime managers, job registries, and SQLite.
- Make the TUI render only daemon projections and event deltas.

### P1 — Required to complete the attached architecture

#### 5. Add first-class interactive, deferred, and detached execution models

The protocol now defines mode-specific durable dispatch receipts for:

- `InteractiveSession`
- `DeferredTask`
- `DetachedJob`

The remaining work is to turn those receipts into complete durable state machines with attachment/detachment, cursor continuation, reattachment, and mode-specific cancellation and recovery semantics. Existing background-job machinery is useful but is not integrated into those state machines.

#### 6. Finish the artifact data plane

The current `ArtifactStore` provides immutable content-addressed objects, hashes, atomic persistence, bounded capacity, and cursor reads. It is not a chunked ingestion service.

Current mismatch:

- `PutArtifact` carries the complete artifact as `Vec<u8>` in one command.
- The socket frame is capped at 64 MiB.
- The artifact store advertises a 4 GiB per-artifact limit.

Required work:

- Begin/upload-chunk/commit/abort commands.
- Streaming hash verification.
- Durable upload leases and restart cleanup.
- Artifact cursors in task and evidence projections.
- Direct spool adoption for terminal and Nmap output without copying through a control-plane frame.

#### 7. Turn `ServiceSupervisor` into a process supervisor

The current supervisor is a generation-fenced health registry. It supports registration, heartbeat, stale detection, failure marking, and removal.

It does not manage:

- Process startup.
- Handshake deadlines.
- Crash observation.
- Exponential backoff.
- Poisoned generations.
- Automatic restart.
- Graceful drain and forced termination.
- Process ownership and cleanup.

Those behaviors exist in pieces for LiteRT and terminal jobs but have not been unified under the control plane.

#### 8. Complete the read-projection model

Implemented projections:

- Engagement summary.
- Provider state.
- Aggregate overload/capacity information.

Missing projections:

- Active dependency graph.
- Interactive/deferred/detached state.
- Worker queue and lease details.
- Model and adapter residency.
- KV-session ownership.
- Background-job progress and stream cursors.
- Evidence index and contradictions.
- Artifact metadata and cursors.
- Memory/indexing backlog.

#### 9. Adopt the universal provider contract in every subsystem

The `ExecutionProvider` trait and `CapabilityManifest` exist. Runtime and native-tool manifest constructors also exist.

Still missing:

- Actual `ExecutionProvider` adapters for `RuntimeManager` and `ExecutionSupervisor`.
- Embedding and memory-indexer providers.
- MCP connector providers.
- Artifact-processor providers.
- Provider-neutral cancellation and recovery adapters.
- End-to-end schema compatibility negotiation during dispatch.

At present, the universal provider system is a valid control-plane abstraction with test providers, not the universal production path.

#### 10. Implement real Buzz and QM adapters

`BuzzIngress` and `QmIngress` currently convert identifiers and request text into `IngressEnvelope` values.

Missing:

- Buzz transport integration.
- QM transport integration.
- Delivery acknowledgement and deduplication at the integration boundary.
- Progress/event-reference publication back to the source system.
- Reconnect and cursor recovery.
- Correlation of source events with engagement events.

They are protocol mapping types, not operational adapters.

#### 11. Enforce single durable-state ownership

`grokd` opens its own engagement SQLite database, event journal, plan store, and artifact store. The existing application still creates and accesses runtime, job, memory, and engagement state outside the daemon.

Required work:

- Move durable writes behind daemon commands.
- Define migration/import for existing session and job state.
- Prevent production clients from opening the daemon-owned SQLite files.
- Reconcile the binary event journal with the requirement that SQLite be exclusively owned by `grokd`.
- Establish one restart and recovery sequence for all durable state.

#### 12. Complete runtime-profile artifact activation

Declarative versioned runtime profiles, an offline linter, and validated daemon startup activation are implemented. Profiles cover deployment/libc targets, protocol ranges, resource ceilings, provider requirements, immutable artifacts, and binary-size budgets.

Remaining work is transactional resolution of declared model, adapter, and tool-bundle hashes before providers start, plus compatibility checks against the live provider manifests.

#### 13. Add full protocol compatibility and slow-consumer tests

The protocol validates one current version and manifest ranges, but it needs:

- N-1/N+1 compatibility fixtures.
- Unknown-field and unknown-command behavior.
- Event cursor replay tests.
- Slow and disconnected subscriber tests.
- Provider reconnect across daemon restart.
- Mixed provider-version dispatch tests.
- Upgrade and downgrade rejection tests.

### P2 — Required for release qualification

#### 14. Run real LiteRT-LM and LoRA validation

The deterministic worker and adapter tests pass, but real GPU validation requires:

- A macOS ARM64 LiteRT-LM bridge library.
- Qwen 2.5 1.5B LoRA-capable LiteRT model artifact.
- VibeThinker 3B LiteRT model artifact.
- Two compatible Qwen LoRA adapters with different observable behavior.
- Supported LoRA rank and exact base-model identity.

Outstanding gates include real generation, resident adapter switching, rollback, reclaimed memory, and cross-adapter KV isolation.

#### 15. Run live external-provider validation

Fixture tests pass for the operational MCP server. Live validation still requires:

- Running Metasploit RPC endpoint.
- Running Neo4j/BloodHound test database.
- Local NVD 2.x feeds.
- Local Exploit-DB checkout and CSV.
- `ffuf` for long-running background enumeration validation.

#### 16. Validate offline embedding profiles

Required local model assets:

- Quantized EmbeddingGemma 300M.
- Optional Qwen3 Embedding 0.6B high-recall profile.

The release gate must prove FTS fallback remains non-blocking while embedding/indexing is unavailable or saturated.

#### 17. Complete sustained load and soak testing

Still outstanding:

- 24-hour continuous-integration soak.
- 72-hour release soak.
- 100,000-chunk retrieval workload.
- Simultaneous model, adapter, indexing, Nmap, `ffuf`, cancellation, crash, and memory-pressure workload.
- Restart testing with active deferred jobs and partially uploaded artifacts.

## Completion matrix

| Architecture item | Current status | Commit status |
|---|---|---|
| Zero-I/O `xai-grok-protocol` | Implemented foundation | Committed in `c5e60e1` |
| Persistent `grokd` binary | Implemented shell; production ownership incomplete | Committed in `c5e60e1` |
| Bounded engagement writer | Implemented with one bounded writer queue; separate admission classes incomplete | Committed in `c5e60e1` |
| Universal provider manifest and trait | Implemented abstraction; subsystem adoption incomplete | Committed in `c5e60e1` |
| Versioned Tasking IR | Implemented types and validation; scheduler/model wiring incomplete | Committed in `c5e60e1` |
| Interactive/deferred/detached split | Typed receipts implemented; durable mode state machines incomplete | Partial |
| Content-addressed artifacts | Whole-object store complete; chunked data plane incomplete | Committed in `c5e60e1` |
| Universal service supervision | Health/generation registry only | Committed in `c5e60e1` |
| Read projections | Basic projections plus durable cursor client stream; existing TUI integration absent | Partial |
| Buzz ingress | Mapping type only | Committed in `c5e60e1` |
| QM ingress | Mapping type only | Committed in `c5e60e1` |
| MCP operational connectors | Implemented and fixture-tested | Committed in `c5e60e1` |
| Session replay CLI | Implemented and tested; depends on excluded audit-ledger edits | Preserved locally, not committed |
| Runtime/native capability manifests | Implemented and tested | Committed in `c5e60e1` |
| Team-client reconnect identity | Implemented in handshake, ingress events, and projections | Implemented after audit |
| Bash-compatible headless control client | Implemented with JSON/JSONL output | Implemented after audit |
| Optional native GUI | Implemented as separate `egui`/`winit`/`glow` artifact | Implemented after audit |
| Typed runtime profiles and linter | Implemented with startup validation; artifact activation incomplete | Partial |
| Compact/musl packaging gates | Implemented; full daemon musl link requires build-host cross C compiler | Partial |
| Real LiteRT/LoRA release validation | External assets required | Not complete |
| 24-hour/72-hour soak | Not run | Not complete |

## What is genuinely implemented and verified

The following local foundation is real and passes focused tests:

- Versioned protocol identifiers, commands, events, manifests, and tasking types.
- Bounded MessagePack command frames over a permission-restricted Unix socket.
- Bounded daemon command queue and bounded engagement writer queue.
- Checksummed append-only event journal with corruption/truncation detection.
- Durable engagement acceptance and dispatch identity fencing.
- Provider capability resolution and bounded per-provider admission.
- Generation-fenced service health records.
- Immutable content-addressed artifact objects with bounded cursor reads.
- Rebuildable basic projections.
- Metasploit RPC, Neo4j, NVD, and Exploit-DB MCP connector implementation.
- Session replay implementation preserved in the checkout but excluded from the scoped commits because it depends on audit-ledger changes.
- Runtime and native-execution capability manifests.

Focused verification performed during this audit:

- `xai-grok-control-plane`: 12 passed, 0 failed.
- `xai-grok-protocol`: 4 passed, 0 failed.
- `xai-grok-engagement`: 16 passed, 0 failed.
- `grok-ops-mcp`: 6 passed, 0 failed.
- Replay tests: 3 passed, 0 failed.
- Runtime capability test: 1 passed, 0 failed.
- `grokd` binary build: passed.

These results prove the local components compile and their focused contracts work. They do not prove the missing production integration described above.

## Recommended implementation order

1. Move runtime, execution, turn coordination, memory, and connector ownership into `grokd`.
2. Add executable provider transport and bootstrap all provider classes.
3. Implement the durable dependency scheduler and execution-mode state machines.
4. Add socket event subscriptions and convert the TUI to a projection client.
5. Finish chunked artifact ingestion and direct spool adoption.
6. Complete service process supervision and recovery.
7. Implement operational Buzz and QM adapters.
8. Complete projections, profiles, and compatibility tests.
9. Run real LiteRT/LoRA, connector, load, and soak qualification.

## Scope boundary

This audit covers architecture, runtime plumbing, capabilities, persistence, IPC, performance, and production integration. It intentionally excludes permissions, approvals, governance, compliance, and operational documentation.
