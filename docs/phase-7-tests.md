Phase 7 Runtime Validation Contract
1. Purpose and constitutional role of this phase

Phase 7 is the final local validation layer before the system moves from constitutional architecture completion into operational cadence deepening, external contract validation, and eventually live testing. This phase is not meant to re-justify the architecture. The architecture has already been established through the decomposition of governance, execution, reconciliation, capability, publishing, acquisition, persistence, telemetry, and retry authority into bounded organisms.

The purpose of Phase 7 is to verify that the system behaves correctly as a whole after kernelization. The question is no longer whether the architecture is valid in theory. The question is whether the runtime, when subjected to realistic state mutations, worker dispatches, cadence pressure, and domain interactions, preserves its constitutional boundaries and produces deterministic, observable, and recoverable results. In other words, Phase 7 is a runtime truth phase. It exists to prove that the model can operate inside the architecture without violating the architecture.

The test layer must therefore operate as a synthetic runtime rather than a collection of isolated unit tests. It must observe the system at the level of event chains, state mutations, worker execution paths, and authority boundaries. The system under test is not a single function. It is the complete behavior of the runtime as expressed through governed state transitions.

2. The theory behind the testing loop

The testing loop must reflect the reality of the system after kernelization. In the old architecture, it was reasonable to test a function such as storeMessage() or normalizeComment() in isolation because those functions were carrying much of the meaning of the system. That model no longer applies. The system is now event-sourced in behavior, bounded in authority, and layered in execution. The meaningful unit of behavior is no longer a function return value. It is a chain of decisions and mutations that begins with an event and ends with a persisted state.

This means the test loop must be structured around runtime causality. An input event enters the runtime. The governance layer evaluates whether the event is legitimate. If the event is legitimate, the correct worker is dispatched through the correct substrate. The worker executes the bounded operation. The substrate governs the mechanics of that operation. The resulting state mutation is persisted. The event bus records the causal sequence. The state inspector confirms that the runtime entered the correct state. If any step in that chain is incorrect, the test must fail even if the underlying function technically succeeded.

This theory is critical because it prevents false positives. A function can succeed while the system fails. A worker can return a valid payload while the wrong state is mutated. A database write can complete while an illegal authority path was taken. A retry can succeed while the wrong cadence semantics were used. Phase 7 exists to detect those failures by validating the whole chain instead of validating fragments of it.

3. The runtime model under test

The system under test is a governed runtime composed of a Constitutional Kernel, bounded domain FSMs, workers, substrates, a persistence layer, a capability plane, a reconciliation plane, a retry plane, and a telemetry plane. Each layer has a narrow responsibility. The FSMs own lifecycle legitimacy. The CK owns authority and routing legitimacy. The workers own bounded execution. The substrates own execution environments and mechanical support. The engines own deterministic primitives. The persistence layer materializes state. The telemetry layer records what happened. The reconciliation layer verifies whether the system remains coherent.

Phase 7 must validate that these layers continue to behave as distinct organisms even when they are executed together in a containerized test environment. The runtime simulator must therefore not merely call code. It must reproduce the interaction of code paths under realistic governance conditions. This is what makes the test layer constitutional rather than procedural.

4. The testing environment as a synthetic runtime

Phase 7 should use a containerized runtime built with Testcontainers and Vitest. The environment should include Postgres, Redis where needed, the governance runtime, the worker harness, the event recorder, the state inspector, and any substrate or kernel needed to simulate the current architecture. This environment must be deterministic enough to support repeatable assertions while still behaving like a real runtime. The point is not to mock away the system. The point is to control the external world so that the runtime can be observed precisely.

Inside this synthetic runtime, the model should not be allowed to bypass governance. Events should flow through the governance simulator. Worker execution should occur through the worker harness. Every emitted event should be recorded. Every state mutation should be inspectable. Every dispatch should be attributable. Every retry should be visible. Every capability transition should be testable. The runtime should be able to idle, recover, and continue across multiple ticks without collapsing into hidden state or unobserved side effects.

The test environment should also support a simulated Graph layer. That layer should not be a trivial mock. It should behave like a deterministic external dependency with realistic endpoints and payload shapes. The goal is to simulate the behavior of Graph sufficiently well that the acquisition, publishing, and capability workers can be tested against representative payloads without introducing live Meta variability. This allows the architecture to be validated before external contract testing begins.

5. What Phase 7 is actually validating

Phase 7 is validating operational correctness. That includes state mutation correctness, event causality correctness, governance correctness, worker dispatch correctness, cadence correctness, replay correctness, persistence correctness, and observability correctness. It is also validating that the architecture does not silently drift under pressure. If the system handles one event properly but drifts after fifty ticks, that is a failure. If the system handles success paths but fails under rate limits or token expiry, that is a failure. If the system persists data but loses causal traceability, that is a failure. If the system obeys governance but cannot recover deterministically, that is a failure.

This phase also validates that the workers are now semantically bounded enough to be trusted with deeper operational intelligence later. If the workers are still too shallow, the tests will reveal that. If the workers are already robust, the tests will confirm it. In either case, the result is actionable because the test layer is designed around system behavior rather than implementation assumptions.

6. Runtime event semantics

Every test in Phase 7 should begin with an event rather than a function call. That event may represent a fetch completion, a capability change, a publishing trigger, a reconciliation tick, a repair event, or a cadence event. The system must then be allowed to respond as a runtime organism. The correctness of the result must be judged by the chain of events produced and the state produced, not by any single intermediary return.

For example, a conversation fetch completion event should not merely verify that a fetch function returned records. It should verify that the runtime observed the event, routed it through governance, dispatched the correct bounded worker, normalized the payload, persisted the correct conversation rows, emitted the expected completion event, and left the system in the correct observability state. The same logic applies to publishing, capability verification, reconciliation, and any future domain kernel.

This is essential because the architecture now separates role and responsibility. A test that only validates a worker in isolation is not sufficient. The runtime must validate whether the worker behaved correctly in context. Context includes governance, cadence, substrate, persistence, and observability.

7. The event recorder as canonical truth during testing

The event recorder must be treated as the canonical source of runtime truth during testing. Every event should be captured with a timestamp, an event type, a source, a destination, a payload snapshot, and any linked state transition. Every worker execution should be captured in the same way. Every state mutation should be recorded as part of the causal timeline. Every test failure should therefore be explainable as a sequence rather than a mystery.

This matters because the architecture is event-driven and constitutionally layered. A runtime failure is rarely caused by one thing. It is usually caused by a sequence of correct-looking choices that together lead to an incorrect outcome. The event recorder is what allows the team to reconstruct that sequence. It turns runtime debugging from guesswork into historical analysis. In a system like this, that is not a luxury. It is a requirement.

8. State mutation as the primary assertion target

Phase 7 should assert state mutations, not just function outputs. This means the test should verify the actual database state, the actual emitted events, the actual governance decisions, and the actual worker transitions. If a message fetch completes, the test should verify that the message row exists, the conversation linkage exists, the correct lineage references exist, and no unintended repair or drift events were emitted. If a publishing event succeeds, the test should verify that the correct post queue or scheduled post state was updated and that the transition was visible to the governance layer.

The important thing is that state mutation must match constitutional expectation. A correct function output with an incorrect state mutation is not a pass. A correct state mutation through an incorrect governance route is also not a pass. This is the difference between testing implementation and testing the runtime.

9. Governance validation as a separate category

Governance should be tested independently from execution. A token expiry, rate limit, capability degradation, or repair request should not cause workers to make governance decisions. Those decisions belong to the FSMs and the CK. Phase 7 must therefore verify that governance consistently chooses the correct action under the correct conditions and that workers do not begin to self-govern under pressure.

This is especially important now that the Graph Capability Plane has been extracted. Capability state should be produced deterministically and then consumed by downstream domains. Acquisition and publishing should not inspect token internals directly. They should consume capability state. The tests must confirm that this separation holds. If a worker can bypass the capability plane or infer its own authority, the architecture has drifted.

10. Cadence validation as a long-running correctness test

Cadence is not merely a scheduler concern. It is a runtime survival concern. Phase 7 should verify that the system can continue to operate across multiple ticks without accumulating hidden corruption, duplicate dispatches, missing transitions, or unobserved retries. This is where long-run tests matter. Running the cadence for ten ticks is useful. Running it for fifty ticks is better. Running it for hundreds of ticks is what reveals whether the runtime is stable or only locally correct.

Cadence tests should observe dispatch frequency, task selection, retry behavior, prioritization, and recovery behavior. The system should be able to continue operating under bounded pressure without losing its state invariants. The aim is not to test whether one tick works. The aim is to test whether the runtime remains itself after many ticks.

11. Worker validation as bounded operational intelligence

The workers are now the main point of operational growth. Phase 7 should validate that each worker is not merely returning data, but is returning data through the correct operational path. Each worker should be verified through its complete runtime behavior, from event receipt through substrate execution to state mutation and observability projection. This is where the system proves that the decomposition work actually produced bounded execution organisms and not just reorganized files.

A worker should be able to handle realistic payloads, partial payloads, degraded payloads, and edge-case payloads without breaking the authority model. It should not self-authorize, it should not route around governance, and it should not mutate unsupported state. The test layer should treat each worker as an operational agent inside a constitutional system, not as a standalone script.

12. Capability plane validation as a constitutional dependency test

The Graph Capability Plane now sits beneath acquisition and publishing as a constitutional dependency. Phase 7 must validate that capability changes propagate correctly and that capability state transitions remain deterministic. A new account connection, a token refresh, an auth strike, or a repeated graph failure should produce the expected capability state. Downstream consumers should then react to that capability state exactly as designed.

This is especially important because the capability plane will later become one of the most critical operational layers in the system. If its output is wrong, acquisition and publishing may fail in ways that look like business bugs but are actually constitutional bugs. Phase 7 prevents that by testing the plane as a first-class runtime organism.

13. Legacy regression behavior

Any bug discovered and fixed during the kernelization effort must become a permanent regression test. This includes issues like missing identifiers during persistence, worker duplication, hidden authority leakage, retry contamination, legacy caller paths, improper state transitions, and projection inconsistencies. The purpose is not just to fix the past. The purpose is to prevent the past from returning under a new file name.

These regression tests are not optional. They are the memory layer of the architecture. Without them, the system will gradually re-accumulate the same semantic debt in slightly altered form. Phase 7 should therefore be treated as the point at which architectural lessons become permanent executable proof.

14. The simulated Graph runtime

The Graph runtime simulator should behave like a deterministic external environment rather than a simplistic mock. It should provide endpoints and payload shapes that resemble the runtime the workers expect to encounter. The value of this simulator is that it lets the workers, substrates, and governance layers be exercised against realistic behavior without using live tokens or real network dependencies.

That simulator is what allows Phase 7 to bridge the gap between pure architecture validation and external contract testing. It proves that the runtime can behave correctly in a realistic environment while still remaining under full test control. Once that passes, moving to contract tests against Meta becomes a much safer step rather than a leap into uncertainty.

15. Observability as a first-class test requirement

A runtime that cannot explain itself cannot be trusted. Phase 7 must therefore validate observability as part of correctness. Every worker must emit start, success, failure, duration, retry, and degradation information. Every governance decision must be recorded. Every mutation must be visible in the state inspector. Every deviation must be reconstructable from the event record.

Observability is not a convenience layer in this architecture. It is part of the proof that the architecture is behaving correctly. If the system is correct but opaque, then it is not ready. The operator must be able to see the chain of causality at the exact layer where it occurred.

16. Success conditions for the phase

Phase 7 is complete when the runtime can be executed repeatedly in the containerized simulator, with real governance, real workers, real substrates, real persistence, and realistic Graph inputs, while maintaining all expected state transitions, causal chains, authority boundaries, and observability guarantees. The tests should demonstrate that the system can withstand repeated operational pressure without drifting from its constitutional model.

At that point the architecture may be considered validated at the runtime level. That does not mean the work is done. It means the architecture has been proven. After that, the next stage is operational cadence improvement, worker intelligence deepening, contract testing against Meta, and eventually live infrastructure testing. But those later stages should sit on top of a Phase 7 runtime that has already proved it can think and act coherently.