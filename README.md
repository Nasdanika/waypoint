# Waypoint Module — Design Document

## 1. Overview

This document specifies a new Nasdanika module — **`org.nasdanika.waypoint`** — providing a generic abstraction for tracking the lineage and payload of an execution as it advances through some structure: a graph, a file system, an EMF model, an AST, a state machine. A `Waypoint` records *where* execution is, *how it got there*, *what data it carries*, and — for richer variants — *which realm it runs against* and *which OpenTelemetry span scopes it*.

The model is inspired by three complementary precedents:

- Workflow / process engine tokens, which carry payload along a process and support rollback / retry through transaction groups.
- **Git** commit history (immutable, parents, fast-forward, merge, reset).
- **Petri-net tokens** and classical workflow semantics.

Originally this was scoped to graph traversal and was going to live alongside `org.nasdanika.graph.message`. On reflection, the abstraction has no inherent dependency on graphs and warrants its own module.

The module also absorbs `Realm`, `ExclusiveRealm`, and `ReadWriteRealm` — interfaces previously in `org.nasdanika.common`. They belong with the waypoint abstraction they serve (see §6.3) and are independently useful for any client that needs a guarded, command-accessed state without lineage.

## 2. Motivation

`OpGraph` (`https://op-graph.models.nasdanika.org/`) needs a way to thread state along an execution path, fork at parallel gateways, join at merges, retry on failure, and emit telemetry. The same need recurs in many places:

| Structure                 | `E` (execution position)        | Typical `S` (state)            |
| ------------------------- | ------------------------------- | ------------------------------- |
| OpGraph                   | `Node`, `Connection`            | Named variable map              |
| Generic graph traversal   | `Element`, `Connection`         | Visited-set / accumulator       |
| File-system walk          | `Path`                          | Accumulated byte count          |
| Ecore traversal           | `EObject`, `EReference`         | `EditingDomain` / `ResourceSet` |
| Compiler pass             | AST node                        | Symbol table                    |
| State machine             | State, transition               | Variables                       |
| HTTP server request flow  | Filter / handler                | Auth context, MDC               |
| NSML mapping execution    | `(EObject source, MappingRule)` | `MappingContext`                |

A single, structure-agnostic vocabulary — waypoint, commit, merge, state, realm, telemetry — covers all of these, and lets specialized engines (OpGraph runtime, [NSML](https://github.com/Nasdanika-Models/nasdanika-semantic-mapping-language/blob/main/README.md) mapping engine, EMF traversal, file walkers) compose freely with the same primitives.

## 3. Prior art and positioning

The waypoint API draws on — and in places deliberately doesn't replicate — several existing bodies of work. This section surveys the landscape and explains why the module exists as a separate thing rather than building on one of the alternatives.

### 3.1 Survey

**Workflow / process-engine tokens.** Camunda, Flowable, and Activiti model a running BPMN process as a tree of `Execution` objects: each execution has a parent, a position (an `ActivityImpl`), local variables, and a lifecycle. jBPM predates them. Temporal (ex-Cadence) takes the same shape into a durable, replayable workflow execution — each `WorkflowExecution` has a complete event history that is essentially our waypoint lineage promoted to a persistent first-class artifact. Apache Airflow's task instances are similar, but the DAG structure is rigid.

**AI-era state graphs.** LangGraph's `StateGraph` carries a typed `State` through nodes, supports sync and async node bodies, has a checkpointer (persistent state), interrupts (pause/resume), conditional edges, and parallel branches with state merging. It is essentially `StateWaypoint<Node, State>` with merge semantics, implemented in Python, bound to its own graph structure. Microsoft Semantic Kernel's Process Framework is converging on the same shape. CrewAI and AutoGen carry context less explicitly.

**Observability / context propagation.** Project Reactor's `Context` is the immutable-threaded-state piece, but has no lineage — it's a leaf in your chain, not a tree. Micrometer `Observation` is much closer to `TelemetryWaypoint`: explicit parent/child, scopes, handlers, designed to wrap arbitrary execution. OpenTelemetry's `Span` does most of `TelemetryWaypoint`'s job; our interface is a thin adapter. SLF4J MDC is mutable thread-local with no lineage.

**Functional state threading.** The State monad in Vavr (Java), Cyclops (Java), Arrow (Kotlin), Cats (Scala). `StateWaypoint#map` is a State-monad bind extended with branching. None of these pairs state threading with execution-position tracking or telemetry, because they are language-level rather than domain-level abstractions.

**Content-addressed history.** jGit, Pijul, Mercurial. Event-sourcing frameworks — Axon, EventStoreDB — express the same idea as causation and correlation IDs on events.

**Specifications.** JSR 352 (Java Batch) `StepContext`, checkpoint, restart. JTA savepoints. Older but the rollback semantics map directly.

### 3.2 What we reuse rather than reinvent

- **OpenTelemetry `Span` / `Context`.** `TelemetryWaypoint` is a thin adapter, not a parallel telemetry system. The `org.nasdanika.waypoint.telemetry` submodule is the only place this matters.
- **Micrometer `Observation`.** An alternative `TelemetryWaypoint` backend for Spring-adjacent consumers — adds nothing to the core module.
- **Reactor `Context`.** We *carry* waypoints through reactive pipelines using it; we do not replicate it.

### 3.3 Why a separate module rather than adopting an existing system

- **Generic over the position type `E`.** Every existing system fixes the position type — BPMN activity, Airflow task, LangGraph node, Temporal workflow. None lets the consumer say "my position is a `Path`," or "my position is an `EObject` plus the `EReference` we traversed". That is the central novelty, and it's the property that lets one set of primitives serve OpGraph, NSML, EMF traversal, and file walks alike.
- **Composable facets.** State, realm, telemetry as orthogonal mixins. Existing systems either bake everything into a monolithic execution object (Camunda `ExecutionEntity`, Temporal workflow handle) or split telemetry off entirely (OpenTelemetry separate from business logic). Neither makes it easy to choose your set of facets per use case.
- **Library, not runtime.** Camunda, Temporal, LangGraph are runtimes — you cede control to their engine. Waypoint is a library — the engine you build calls into it. This matches OpGraph's positioning: you embed it; it doesn't embed you.

### 3.4 Scope and positioning

The contribution is in the *factoring* — structure-agnostic position, composable facets, reactive-streams-native primitives — not in any single primitive. That is enough to justify the module inside the Nasdanika orbit, where there is concrete downstream demand (OpGraph runtime, NSML execution, EMF traversal, file walks). It is probably *not* enough to justify the module as a standalone library competing for adoption against Micrometer, LangGraph, and Temporal. The published module aims to be useful to anyone who happens to need its shape; it does not chase ecosystem competition.

## 4. Module Layout

```
org.nasdanika.waypoint                       (core — this document)
org.nasdanika.waypoint.telemetry              (OpenTelemetry integration)
org.nasdanika.waypoint.git                    (jGit-backed history persistence)
org.nasdanika.waypoint.emf                    (ResourceSet-backed state, change recording)
org.nasdanika.waypoint.emf.transaction        (EMF Transaction-based RealmWaypoint)
org.nasdanika.waypoint.graph                  (Adapters for org.nasdanika.graph)
org.nasdanika.waypoint.nsml                   (NSML mapping execution — see §11)
```

Core depends only on the JDK, `org.reactivestreams`, and `reactor-core` (for `Mono` / `Flux`). Every other technology lives in an optional submodule so that downstream consumers pay only for what they use.

## 5. Naming alignment

Method and type names borrow from jGit and from `java.util.stream` deliberately. The intent is that someone fluent in Git or in the Streams API recognises the verbs without explanation.

| Concept              | jGit term                  | This API                    | Rationale                                                       |
| -------------------- | -------------------------- | --------------------------- | --------------------------------------------------------------- |
| Predecessor commits  | `getParents()`             | `getParents()`              | Direct match.                                                   |
| One parent by index  | `getParent(int)`           | `getParent(int)`            | `RevCommit#getParent(int)`.                                     |
| Parent count         | `getParentCount()`         | `getParentCount()`          | `RevCommit#getParentCount()`.                                   |
| Successor commits    | computed via `RevWalk`     | `getChildren()` (opt-in)    | Git doesn't store children; forward traversal here needs them.  |
| Element / position   | (n/a)                      | `getElement()`              | Structural payload — node, file, EObject.                       |
| Append (1 parent)    | `CommitBuilder.commit()`   | `commit(E)`                 | Verb form, mirrors Git fast-forward.                            |
| Append (n parents)   | merge commit               | `merge(E, parents)`         | Same verb as Git merge.                                         |
| Identity             | `getId()` → `ObjectId`     | `getId()` (optional)        | For correlation, persistence, equality.                         |

The draft's `createChild(...)` is replaced by the two verbs `commit` and `merge`. They carry the Git mental model intact and avoid the noun-y, structure-irrelevant "child".

### 5.1 Sync and async on the same interface

Every interface in this module exposes both synchronous and asynchronous variants of the same operation. The async variant returns a cold `Mono` (Project Reactor), suffixed `Async`: `execute` / `executeAsync`, `apply` / `applyAsync`, `commit` / `commitAsync` (where applicable), `adapt` / `adaptAsync`. This mirrors the Azure OpenAI Java SDK convention and several other contemporary Java APIs — one interface, two modalities. Callers pick the modality at the call site; implementations are free to share code between them.

The principle: *if an operation can be expressed in both modalities, it should be.* The waypoint module never forces async-only or sync-only on a caller.

## 6. Core interfaces

### 6.1 `Waypoint<E>`

```java
public interface Waypoint<E> {

    /** The element this waypoint records execution at. */
    E getElement();

    /**
     * Parents of this waypoint in creation order. Empty list iff this is a
     * root waypoint. Single-element list for linear progressions, multiple
     * elements for joins / merges.
     */
    List<? extends Waypoint<E>> getParents();

    /** Equivalent to {@code getParents().size()}. Matches jGit. */
    default int getParentCount() {
        return getParents().size();
    }

    /** Parent at index {@code n}. Matches jGit. */
    default Waypoint<E> getParent(int n) {
        return getParents().get(n);
    }

    /**
     * Children of this waypoint. May be empty if the implementation does
     * not track children (which is the default — see §16 Open Questions).
     * Unlike Git commits, we expose children because the typical
     * traversal-time use case reads them forward.
     */
    Collection<? extends Waypoint<E>> getChildren();

    /** Single-parent (fast-forward) descendant. */
    Waypoint<E> commit(E next);

    /**
     * Multi-parent (merge) descendant. {@code this} is the first parent;
     * {@code additionalParents} follow in iteration order.
     *
     * @param next               element of the new waypoint
     * @param additionalParents  joining parents, may be empty
     */
    Waypoint<E> merge(E next, Iterable<? extends Waypoint<E>> additionalParents);

    /**
     * Varargs convenience over {@link #merge(Object, Iterable)}.
     * See §7 for the varargs/generics caveat.
     */
    @SuppressWarnings({"unchecked", "varargs"})
    default Waypoint<E> merge(E next, Waypoint<E>... additionalParents) {
        return merge(next, Arrays.asList(additionalParents));
    }
}
```

### 6.2 `StateWaypoint<E, S>`

Adds a state value of type `S`. State is read-only from the waypoint's perspective; transformations produce *new* waypoints. This matches reactive-streams / functional style — waypoints are values, not mutable cells. (The escape hatch for genuinely mutable state is `RealmWaypoint`, §6.4.)

```java
public interface StateWaypoint<E, S> extends Waypoint<E> {

    /** The state value carried by this waypoint. */
    S getState();

    // ── Narrowed returns ─────────────────────────────────────────────

    @Override
    List<? extends StateWaypoint<E, ?>> getParents();

    @Override
    Collection<? extends StateWaypoint<E, ?>> getChildren();

    /** Fast-forward that propagates the current state unchanged. */
    @Override
    default StateWaypoint<E, S> commit(E next) {
        return commit(next, getState());
    }

    /**
     * Note: Java doesn't allow widening parameter types on override,
     * so the inherited {@code merge(E, Iterable<? extends Waypoint<E>>)}
     * stays with that signature. Implementations check parents are
     * {@code StateWaypoint} at runtime if needed.
     */
    @Override
    StateWaypoint<E, S> merge(E next, Iterable<? extends Waypoint<E>> additionalParents);

    // ── State-aware factories ────────────────────────────────────────

    /** Fast-forward with a new state. */
    <T> StateWaypoint<E, T> commit(E next, T nextState);

    /** Merge with an explicit new state. */
    <T> StateWaypoint<E, T> merge(
            E next,
            T nextState,
            Iterable<? extends StateWaypoint<E, ?>> additionalParents);

    // ── Mapping ──────────────────────────────────────────────────────

    /**
     * Compute the next state from this waypoint and joining parents,
     * synchronously, on the calling thread.
     *
     * The list passed to the mapper has {@code this} as element 0;
     * joining parents follow in iteration order.
     */
    <T> StateWaypoint<E, T> map(
            E next,
            Function<? super List<? extends StateWaypoint<E, ?>>, ? extends T> mapper,
            Iterable<? extends StateWaypoint<E, ?>> additionalParents);

    /**
     * Asynchronous counterpart of {@link #map}. The mapper returns a
     * {@code Mono<T>}; the result is a {@code Mono<StateWaypoint<E,T>>}.
     */
    <T> Mono<StateWaypoint<E, T>> mapAsync(
            E next,
            Function<? super List<? extends StateWaypoint<E, ?>>, ? extends Mono<T>> mapper,
            Iterable<? extends StateWaypoint<E, ?>> additionalParents);
}
```

Notes:

- Mapper signatures use PECS variance (`? super X`, `? extends Y`) — same convention as `Stream#map`, `Stream#collect`, `Collectors`.
- Async returns are `Mono` / `Mono<Void>`, never `CompletionStage`. Adapters can be added later via `Mono#toFuture` / `Mono.fromFuture`.

### 6.3 The Realm family

For state that *cannot* be propagated by value: an `EditingDomain`, a JDBC `Connection`, a `Repository`, an event-loop `Context`, a JFace `Realm`. Clients don't manipulate the state directly — they submit **commands** that execute against it inside the realm's invariants (synchronization, transactions, undo recording).

A `Realm<S>` is a bounded space of state whose invariants — threading, transactions, undo — require that access go through commands rather than direct mutation. The interface is freestanding (it has clients that need a guarded space without lineage) and `RealmWaypoint<E, S>` extends it (§6.4).

The mental model mirrors:

- `org.eclipse.core.databinding.observable.Realm.exec(Runnable)`.
- `EditingDomain.getCommandStack().execute(Command)`.
- `Vertx.runOnContext` / `Context.runOnContext`.

These three interfaces — `Realm`, `ExclusiveRealm`, `ReadWriteRealm` — move into the waypoint module from `org.nasdanika.common`, where they were experimental. They are consolidated here because that is where their primary client (`RealmWaypoint`) lives, and because `org.nasdanika.common` should remain minimal.

#### 6.3.1 `Realm<S>`

```java
public interface Realm<S> {

    /** Run a command against the guarded state; returns when the command completes. */
    void execute(Consumer<? super S> command);

    /** Asynchronous {@link #execute}. */
    Mono<Void> executeAsync(Consumer<? super S> command);

    /** Compute a value from the guarded state. */
    <T> T apply(Function<? super S, ? extends T> command);

    /**
     * Asynchronous {@link #apply}. The command returns a {@code Mono<T>};
     * the realm flattens it. Same shape as {@code Mono#flatMap}.
     */
    <T> Mono<T> applyAsync(Function<? super S, ? extends Mono<T>> command);

    // ── Adaptation ───────────────────────────────────────────────────

    /**
     * View this realm as a {@code Realm<T>}. The adapter runs inside
     * this realm on every command access — lens semantics, not snapshot.
     * Construction is cheap; per-access cost is the adapter cost.
     *
     * The returned realm preserves this realm's threading and transaction
     * discipline; it just substitutes T-shaped state for S-shaped.
     */
    <T> Realm<T> adapt(Function<? super S, ? extends T> adapter);

    /**
     * Async-adapter variant. The adapter runs once asynchronously; the
     * resulting {@code Realm<T>} wraps the produced T as a snapshot.
     * Snapshot rather than lens because re-running an async adapter on
     * every command access is rarely what callers want; if you do want
     * that, write a {@link #adapt} that delegates to a sync helper which
     * blocks on the async source.
     */
    <T> Mono<Realm<T>> adaptAsync(Function<? super S, ? extends Mono<T>> adapter);
}
```

The `Function<? super S, ? extends T>` / `Function<? super S, ? extends Mono<T>>` adapter type is intentionally general. The adapter can be a field selection, a projection, an NSML transformation, an OpGraph mapping, an AI-agent invocation that supplies semantic context, or any composition of these. The most consequential use case — *NSML transformations as adapters* — is worked out in §11.6, because it is what makes `Realm` and `Waypoint` more than independent abstractions sharing a module: they enable each other's most interesting compositions. Sync `adapt` suits cheap deterministic adapters; async `adaptAsync` suits expensive or AI-driven ones where snapshot semantics are correct.

#### 6.3.2 `ExclusiveRealm<S>`

```java
public interface ExclusiveRealm<S> extends Realm<S> {

    /*
     * Refinement of Realm with single-concurrent semantics: at most one
     * execute or apply (sync or async) is in-flight at a time. Subsequent
     * operations queue until the in-flight one completes.
     *
     * No additional methods are required for the basic contract; the
     * type carries the semantics. Implementations may add a
     * {@code tryExecute} / {@code tryApply} pair for non-blocking
     * attempts, and a {@code Disposable} hook for cancellation —
     * deferred to follow-up.
     */

    @Override
    <T> ExclusiveRealm<T> adapt(Function<? super S, ? extends T> adapter);

    // adaptAsync inherited; returns Mono<Realm<T>>. The async snapshot
    // does not need exclusive semantics by default. A caller who wants
    // an exclusive snapshot can wrap the resulting Realm<T> via a small
    // utility (Realms.exclusive(Realm<T>)) — TBD.
}
```

#### 6.3.3 `ReadWriteRealm<S>`

```java
public interface ReadWriteRealm<S> {

    /*
     * Does NOT extend Realm. Clients route reads and writes explicitly
     * via getReadRealm() / getWriteRealm(); the type itself does not
     * expose execute/apply directly. This makes the read/write contract
     * visible in the type system, in the spirit of
     * java.util.concurrent.locks.ReentrantReadWriteLock and EMF
     * Transaction's read/write distinction.
     */

    /**
     * View for shared concurrent reads. Multiple read operations may run
     * concurrently; reads block while a write is in progress.
     */
    Realm<S> getReadRealm();

    /**
     * View for exclusive writes. Writes serialize against each other and
     * against in-flight reads.
     */
    ExclusiveRealm<S> getWriteRealm();

    // ── Adaptation ───────────────────────────────────────────────────

    /**
     * Adapt to a {@code ReadWriteRealm<T>}. Reads and writes on the
     * adapted realm apply the adapter inside the corresponding view of
     * the underlying realm — lens semantics, preserved across read and
     * write.
     */
    <T> ReadWriteRealm<T> adapt(Function<? super S, ? extends T> adapter);

    /** Async-adapter variant — snapshot semantics, see {@link Realm#adaptAsync}. */
    <T> Mono<ReadWriteRealm<T>> adaptAsync(Function<? super S, ? extends Mono<T>> adapter);
}
```

A `ReadWriteRealm<S>` is *not* a `Realm<S>`. The reason is honesty: when a caller has a `Realm<S>` reference they don't know whether the operation they're about to issue is a read or a write, and the realm has no way to route it correctly. Forcing the caller to ask for `getReadRealm()` or `getWriteRealm()` puts the right knowledge at the right place.

### 6.4 `RealmWaypoint<E, S>`

A waypoint over a realm-managed state. Realm methods are inherited; no new methods are needed beyond the narrowed Waypoint returns.

```java
public interface RealmWaypoint<E, S> extends Waypoint<E>, Realm<S> {

    @Override
    List<? extends RealmWaypoint<E, S>> getParents();

    @Override
    Collection<? extends RealmWaypoint<E, S>> getChildren();

    @Override
    RealmWaypoint<E, S> commit(E next);

    @Override
    RealmWaypoint<E, S> merge(E next, Iterable<? extends Waypoint<E>> additionalParents);

    // execute, executeAsync, apply, applyAsync, adapt, adaptAsync
    // inherited from Realm<S>. Closure-capture of the waypoint inside
    // the command body removes any need for BiConsumer<Waypoint, S>
    // ergonomic overloads — callers write
    //     wp.execute(state -> doStuff(wp, state));
    // and the compiler does the right thing.
}
```

A draft of `RealmWaypoint` had `execute(BiConsumer<RealmWaypoint<E,S>, S>)` etc. — passing both waypoint and state explicitly. That's not needed once `Realm` is its own interface: the caller's closure captures the waypoint reference. The simpler signatures inherited from `Realm` are sufficient.

#### Should `RealmWaypoint` extend `StateWaypoint`?

It has state. But clients aren't expected to call `getState()` directly — they go through `execute` / `apply` to respect the realm's threading and transaction rules. Reading state outside a command can be unsafe.

**Recommendation:** keep them as siblings. Provide a combined interface `RealmStateWaypoint<E, S>` for the cases where direct state read is *also* safe (immutable snapshot state, for instance). This keeps the API honest about thread-safety; clients that genuinely want both opt in by typing against the combined interface.

### 6.5 `TelemetryWaypoint<E>`

Wraps an OpenTelemetry `Span` and `Context`. Each `commit` / `merge` corresponds to a child span; `close()` ends the span. The waypoint is `AutoCloseable` so it works in try-with-resources.

```java
public interface TelemetryWaypoint<E> extends Waypoint<E>, AutoCloseable {

    @Override
    List<? extends TelemetryWaypoint<E>> getParents();

    @Override
    Collection<? extends TelemetryWaypoint<E>> getChildren();

    @Override
    TelemetryWaypoint<E> commit(E next);

    @Override
    TelemetryWaypoint<E> merge(E next, Iterable<? extends Waypoint<E>> additionalParents);

    /**
     * OpenTelemetry context for propagation across process boundaries
     * (HTTP headers, message attributes).
     */
    Context getContext();

    /**
     * Tracer for instrumenting code that wants to create spans
     * beneath this waypoint's span without creating a child waypoint.
     */
    Tracer getTracer();

    /** Optional meter, for waypoints that also scope metrics. */
    Meter getMeter();

    /** End the span associated with this waypoint. Idempotent. */
    @Override
    void close();

    /** Async close — when the span lifecycle ends with an async op. */
    Mono<Void> closeAsync();

    /** Run a command with this waypoint's context activated. */
    void execute(Consumer<? super TelemetryWaypoint<E>> command);

    Mono<Void> executeAsync(Consumer<? super TelemetryWaypoint<E>> command);

    <T> T apply(Function<? super TelemetryWaypoint<E>, ? extends T> command);

    <T> Mono<T> applyAsync(
            Function<? super TelemetryWaypoint<E>, ? extends Mono<T>> command);
}
```

The `applyAsync` mapper returns `Mono<T>` and the method returns the flattened `Mono<T>` — the same shape as `Mono#flatMap`, avoiding an awkward `Mono<Mono<T>>`.

## 7. Generic varargs

`Waypoint<E>... parents` produces an *"unchecked generic array creation for varargs parameter"* warning at every call site. `@SafeVarargs` requires the annotated method to be `final`, `static`, or `private` — none of which applies to a regular interface method. It *can* be applied to a `private` interface method in Java 9+, but not to a `default` one.

Options considered, in order of preference:

1. **Drop the varargs from the interface entirely.** Only declare the `Iterable` overload. Callers use `List.of(a, b)` or `Set.of(a, b)`. Cleanest, most reactive-streams-style.
2. **Keep a `default` varargs convenience method** that delegates to the `Iterable` overload, with `@SuppressWarnings({"unchecked","varargs"})` on the declaration. Call sites stay clean.
3. **Static varargs factory in a `Waypoints` utility class** that *is* `final` and *can* legally bear `@SafeVarargs`.

**Recommendation:** option 2 — pragmatic, idiomatic with `List.of` / `Stream.of`, and the warning is suppressed exactly once at the declaration site.

## 8. Combinations of facets

We need waypoints that are both telemetric and stateful, or both telemetric and realm-bound. Two strategies, ship both.

### 8.1 Composed interfaces (typed)

```java
public interface TelemetryStateWaypoint<E, S>
        extends TelemetryWaypoint<E>, StateWaypoint<E, S> {

    @Override
    List<? extends TelemetryStateWaypoint<E, ?>> getParents();

    @Override
    TelemetryStateWaypoint<E, S> commit(E next);
    // ...narrowed returns
}

public interface TelemetryRealmWaypoint<E, S>
        extends TelemetryWaypoint<E>, RealmWaypoint<E, S> { /* ... */ }

public interface RealmStateWaypoint<E, S>
        extends RealmWaypoint<E, S>, StateWaypoint<E, S> { /* ... */ }
```

Pro: single object, strong typing at call sites.
Con: combinatorial growth when facets multiply (auth context, MDC, transaction handle, …). Bridge methods are needed to narrow return types, and default methods can clash between super-interfaces.

### 8.2 Facet pattern (open-ended)

```java
public interface Waypoint<E> {
    // ...
    default <F> Optional<F> facet(Class<F> facetType) {
        return Optional.empty();
    }
}

// Usage:
waypoint.facet(TelemetryFacet.class)
     .ifPresent(t -> t.span().addEvent("..."));
waypoint.facet(StateFacet.class)
     .ifPresent(s -> useState(s.value()));
```

Pro: open-ended; aligns with Eclipse `IAdaptable` and EMF `EObject.eAdapters()` — both already used by Nasdanika.
Con: less type-safe; needs explicit lookup.

**Recommendation:** ship both. Composed interfaces cover the common cases with strong typing; `facet(...)` keeps `Waypoint<E>` future-proof.

## 9. Waypoint factories

A `WaypointFactory<E, W>` produces the root waypoint(s) and encapsulates traversal-wide policy (child tracking on/off, threading model, telemetry hookups, …).

```java
public interface WaypointFactory<E, W extends Waypoint<E>> {

    /** New root with no parents. */
    W root(E element);

    /**
     * New root with the given parents. Useful for resuming a traversal
     * or splicing sub-traversals together.
     */
    W root(E element, Iterable<? extends W> parents);
}

public interface StateWaypointFactory<E, S>
        extends WaypointFactory<E, StateWaypoint<E, S>> {

    StateWaypoint<E, S> root(E element, S initialState);
}

public interface RealmWaypointFactory<E, S>
        extends WaypointFactory<E, RealmWaypoint<E, S>> { /* ... */ }

public interface TelemetryWaypointFactory<E>
        extends WaypointFactory<E, TelemetryWaypoint<E>> {

    TelemetryWaypoint<E> root(E element, Tracer tracer);
}
```

A `Waypoints` utility class hosts decorators and static helpers, and is the right place for `@SafeVarargs` static varargs methods (see §7):

```java
public final class Waypoints {
    private Waypoints() {}

    public static <E> WaypointFactory<E, Waypoint<E>> recording();

    public static <E, S> StateWaypointFactory<E, S> stateful();

    public static <E, W extends Waypoint<E>> WaypointFactory<E, W> withChildTracking(
            WaypointFactory<E, W> delegate);

    @SafeVarargs
    public static <E> List<Waypoint<E>> parents(Waypoint<E>... parents) {
        return List.of(parents);
    }
}
```

A small companion utility class `Realms` is the equivalent for realm operations that need static helpers (e.g. an `exclusive(Realm<T>)` decorator that wraps a non-exclusive realm in an exclusive one):

```java
public final class Realms {
    private Realms() {}

    public static <S> ExclusiveRealm<S> exclusive(Realm<S> delegate);

    public static <S> ReadWriteRealm<S> readWrite(
            Realm<S> readView, ExclusiveRealm<S> writeView);
}
```

## 10. Technology-specific submodules

### 10.1 `org.nasdanika.waypoint.telemetry`

Reference `TelemetryWaypoint` implementation backed by the OpenTelemetry SDK:

- Span creation per `commit` / `merge`. Single-parent → `Span.setParent`; multi-parent → primary parent via `setParent`, joining parents via `Span.addLink`.
- Context propagation across process boundaries via `TextMapPropagator`.
- Optional `Meter` for metrics scoped to the span.

### 10.2 `org.nasdanika.waypoint.git`

`GitWaypoint<E, S>` persists each commit to a Git repository — element + state are serialized to blobs and recorded as a real `RevCommit`. Branches and merges happen at the jGit level. Use cases: durable execution history for long-running or replayable processes, rollback via `git reset` / `git revert`, forensic analysis.

```java
public interface GitWaypoint<E, S> extends StateWaypoint<E, S> {
    ObjectId getCommitId();
    RevCommit getCommit();
    Ref getBranch();
}

public final class GitWaypointFactory<E, S>
        implements StateWaypointFactory<E, S> {

    public GitWaypointFactory(
            Repository repository,
            String branch,
            Serializer<E> elementSerializer,
            Serializer<S> stateSerializer) { /* ... */ }
}
```

#### Trace-context propagation — weaving Git into the larger trace

Beyond serializing element and state, each `GitWaypoint` commit carries the **OpenTelemetry trace context** of the span that produced it, recorded as commit trailers in the W3C Trace Context format (`traceparent`, `tracestate`). The commit's trailer block is just another `TextMapPropagator` carrier — the same mechanism §10.1 uses for HTTP headers and message attributes — so injection on commit and extraction on read reuse the OpenTelemetry SDK rather than a bespoke encoding:

```
Waypoint-Id: 3f9a1c…
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
tracestate: nsml=rule:order-mapping
```

This unifies the Git-persisted execution with the wider distributed trace. A commit written under span S records S's `traceparent`; any process that later reads the commit — to resume after an escalation (§14), to replay, or for forensic analysis — extracts that context and creates its spans as children or links of S. The trace therefore spans not just process and machine boundaries but **time**: the in-memory `TelemetryWaypoint` spans (§6.5), the upstream distributed trace, and the durable Git history all share trace and span ids, so an observability backend shows one continuous trace from the originating request, across the cold gap of a human-in-the-loop pause (§14), to final completion. Merge commits carry one `traceparent` per parent as span links — the same primary-parent / `addLink` split as §10.1 — so the commit DAG and the trace DAG reinforce rather than duplicate each other. The recorded trace and span ids also double as a join key in both directions: pivot from a commit in the repository to its trace in the observability backend, and from a span back to the commit that persisted it.

### 10.3 `org.nasdanika.waypoint.emf`

Bridges to EMF `ResourceSet` / `Resource`:

- `ResourceSetStateWaypoint<E>` — `S = ResourceSet`; child waypoints see snapshots via copying.
- `ChangeRecordingStateWaypoint<E>` — wraps `org.eclipse.emf.ecore.change.util.ChangeRecorder`; each step records a `ChangeDescription` that can be applied forward or applied-inverse (rollback).

### 10.4 `org.nasdanika.waypoint.emf.transaction`

Bridges to `org.eclipse.emf.transaction`. Commands issued via `RealmWaypoint#execute` are wrapped in `RecordingCommand`s and pushed to the editing domain's command stack, so EMF undo/redo and write-locks work transparently. The EMF `TransactionalEditingDomain`'s read/write distinction maps naturally onto `ReadWriteRealm` (§6.3.3).

### 10.5 `org.nasdanika.waypoint.graph`

Adapters for `org.nasdanika.graph`. Generic `Element` (node/connection) is the natural `E`. This is the original OpGraph use case; it lives here rather than in core so the core module has no graph dependency.

## 11. Mapping and transformation case study

A worked example tying the module together, and the reason `org.nasdanika.waypoint.nsml` (and an analogous integration into NSML itself) is on the roadmap.

NSML — the Nasdanika Semantic Mapping Language — describes transformations from source models to target/semantic models via rules. An execution of an NSML mapping is a traversal of the source structure that produces target structure. Each rule firing is an execution step. The fit with waypoints is direct.

### 11.1 Mapping execution as waypoints

**Position (`E`).** A `MappingStep` value combining the source element being mapped and the rule that fired:

```java
record MappingStep(EObject source, MappingRule rule) {}
```

`E = MappingStep` gives the lineage immediate meaning — every waypoint says "rule R was applied to source S."

**State (`S`).** A `MappingContext` holding:

- The target `ResourceSet` being built.
- A bindings map: source `EObject` → target `EObject`(s).
- Accumulated diagnostics (errors, warnings, low-confidence flags from AI rules).
- Source-tracing references for downstream provenance queries.

In practice the mapping context is realm-managed — writes to the target `ResourceSet` go through commands — so `RealmStateWaypoint<MappingStep, MappingContext>` is the right shape. Reading the bindings map for routing decisions is safe; writes go through `execute`.

**Telemetry.** Each `commit` / `merge` is a span. Rule attributes go on the span: `nsml.rule.id`, `nsml.rule.confidence`, `nsml.source.uri`. Rule bodies that call AI agents emit child spans with OpenTelemetry gen-ai semantic-convention attributes (`gen_ai.system`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`). The waypoint tree *is* the trace tree.

**Lineage as provenance.** "Why does this target element exist?" is answered by walking parents from the waypoint that produced it. The answer is structurally available, not reconstructed from logs. Every target element carries a reference (an EAnnotation, an EMF Adapter, or a side index) to the waypoint whose rule firing produced it.

### 11.2 Worked sketch

```java
// Engine startup
var factory = new NsmlWaypointFactory(targetResourceSet, openTelemetry);
RealmStateWaypoint<MappingStep, MappingContext> root = factory.root(
        new MappingStep(null, null),       // synthetic root step
        MappingContext.fresh());

// A deterministic rule firing
var step = new MappingStep(sourceEObject, rule);
RealmStateWaypoint<MappingStep, MappingContext> child = root.commit(step);
child.execute(ctx -> {                       // closure captures `child`
    var targetEObject = rule.apply(sourceEObject, ctx);
    ctx.bind(sourceEObject, targetEObject);
    ctx.annotateProvenance(targetEObject, child);
});

// An AI-backed rule
RealmStateWaypoint<MappingStep, MappingContext> agentStep =
        child.commit(new MappingStep(otherSource, agentRule));
agentStep.applyAsync(ctx -> agent
        .chooseTargetConcept(otherSource, ctx)
        .doOnNext(answer -> ctx.recordConfidence(agentStep, answer.confidence()))
        .map(answer -> answer.targetConcept()))
    .subscribe(/* consumer */);

// Retry on low confidence — sibling under the same parent
var lastAnswer = agentStep.<Answer>apply(ctx -> ctx.lastAnswer(agentStep));
if (lastAnswer.confidence() < 0.6) {
    var retryStep = child.commit(
            new MappingStep(otherSource, agentRule.withHigherTemperature()));
    // ...retry on the new branch; old branch retained for audit
}
```

### 11.3 Why async + AI agents fit naturally

Three properties of the waypoint API matter for agentic rules, and all of them come from the core design rather than from special-purpose orchestration:

1. **Real asynchrony with backpressure.** `applyAsync` returns a cold `Mono`; agents are I/O-bound; fan-out (`Flux.merge`, `Flux.concatMap`) is governed by Reactor's standard backpressure mechanics.
2. **Retry from a known-good state.** `pred.commit(retryStep)` snaps back to a known-good waypoint. Retries with different prompts, temperatures, or models become siblings under the same parent. Nothing is lost from the audit trail — the rejected attempt remains in the waypoint tree.
3. **Observable cost and latency.** Per-step OTEL spans capture model, token usage, latency, cost. Aggregated across a mapping run, you get per-rule and total AI cost of producing the target model — which matters in production.

### 11.4 Declarative and agentic rules — uniform shape

A deterministic NSML rule ("if source is `UMLClass`, create `OOPClass`") and an agentic rule ("ask the model to choose the best target concept given source and surrounding context") produce identical execution records — same lineage, same telemetry shape, same retry semantics. The only difference is in the rule body. That's a clean factoring you don't get if agent invocation is built into a special-purpose orchestrator: declarative and agentic transformations are no longer different kinds of system, just different rule bodies.

### 11.5 Submodule sketch

```
org.nasdanika.waypoint.nsml
├── NsmlWaypoint                    // alias for RealmStateWaypoint<MappingStep, MappingContext>
├── NsmlWaypointFactory             // root waypoints, engine wiring, OTEL hookup
├── MappingStep                     // (sourceEObject, rule)
├── MappingContext                  // target ResourceSet + bindings + diagnostics
├── ProvenanceAnnotator             // links target elements to the producing waypoint
└── agent
    ├── AgentRule                    // base for AI-backed rules
    └── GenAiSpanAttributes          // OTEL gen-ai semantic-convention helpers
```

### 11.6 NSML as a Realm adapter

NSML transformations — and OpGraph mappings, and agentic semantic transformations more broadly — fit naturally as the adapter function passed to `Realm.adapt` / `Realm.adaptAsync`. The pattern is: you have a `Realm<S>` over some source representation; you want a `Realm<T>` view over a derived or semantic representation; the derivation is itself an NSML transformation that may be deterministic, async, or agentic.

```java
// Source realm: a guarded source model (EditingDomain, database connection,
// in-memory model — anything realm-managed).
Realm<SourceModel> sourceRealm = ...;

// An NSML transformation, async because some rules are agentic.
NsmlTransformation<SourceModel, TargetModel> transform = NsmlEngine.compile(rules);

// Get a semantic view via the transformation. The transformation runs once
// asynchronously; the resulting Realm<TargetModel> wraps that snapshot.
Mono<Realm<TargetModel>> targetView =
        sourceRealm.adaptAsync(transform::applyAsync);

// Use the semantic view as if it were a real realm.
targetView.flatMap(view ->
        view.applyAsync(target -> downstream.process(target)))
    .subscribe();
```

When the transformation is deterministic and cheap, the synchronous form works:

```java
Realm<TargetModel> liveView = sourceRealm.adapt(cheapTransform::apply);
liveView.execute(target -> ...);   // re-applies the transformation per access
```

In practice almost any non-trivial NSML transformation — and certainly any agentic one — should use `adaptAsync`:

- Re-running a substantive NSML transformation on every Realm access is expensive even when deterministic.
- Re-running an agentic transformation is both expensive *and* non-deterministic — the agent may give a different answer on each invocation, which is rarely what the caller wants.
- The NSML transformation's own execution is internally a `RealmStateWaypoint` chain (§11.1); snapshotting once gives you a single, complete, auditable execution record for the adaptation rather than a parade of partial records, one per access.

This composes recursively. The snapshot `Realm<TargetModel>` returned by `adaptAsync` can itself be adapted further (e.g. a second NSML pass), used as the state for a `StateWaypoint` chain, or passed to a sub-engine. The NSML transformation that produced it is observable via OpenTelemetry, retryable via waypoint sibling branches (§11.3), and persistable via the Git submodule. At every layer, the semantic content of the source becomes a Realm.

The general pattern — *NSML as a view definition language over realm-managed state* — is the strongest single argument for why `Realm` and `Waypoint` live in the same module. They are not merely composable; the combination is more interesting than either part alone, and the combination is what specialized engines (NSML, OpGraph, agentic semantic pipelines) actually need.

## 12. Rollback and retry

The waypoint graph itself is append-only; rollback is achieved by:

1. Locate a waypoint at the desired pre-failure point (`pred`).
2. Create a new child of `pred` for the retry: `pred.commit(retryElement)`.
3. For `StateWaypoint`, `pred.getState()` is the starting state for the retry.
4. For `GitWaypoint`, this maps to `git reset --hard <pred-commit>` and continuing.
5. For `ChangeRecordingStateWaypoint`, inverse-apply the `ChangeDescription`s back to `pred` before retrying.

Failure handling is deliberately *not* part of the core `Waypoint<E>` interface — different traversal engines have different failure semantics (best-effort, transactional, compensating). They build on top of these primitives.

## 13. Memory tiers, checkpointing, and durability for agents

Agent architecture distinguishes **hot memory** — the fast, volatile working state an agent reads and writes on every step — from **cold memory** — the durable, slower store used for recovery, audit, and resumption. Waypoint expresses this not as two subsystems but as a single continuum selected by configuration: the agent code that issues `commit` / `merge` / `execute` / `apply` is identical whether its state lives only in RAM or is committed to Git and replicated to a remote. This section makes that mapping explicit; nothing here is new mechanism, only a naming of what §10 and §12 already provide.

### 13.1 The hot↔cold switch is configuration, not code

A traversal's durability is fixed by which `WaypointFactory` (§9) it is rooted with — the agent never sees the difference:

| Configuration                                            | Tier              | What it buys                                                                 |
| -------------------------------------------------------- | ----------------- | ---------------------------------------------------------------------------- |
| In-memory `StateWaypoint` / `RealmWaypoint`              | **Hot**           | Lowest latency working state; lost on process exit.                          |
| EMF `ResourceSet`, unsaved (§10.3)                       | **Hot** (structured) | Hot, but with change recording and realm discipline already in place.     |
| EMF resource saved to the working tree                   | **Warm**          | Survives a crash; no curated history, no rollback points.                    |
| `GitWaypoint` commit, local repository (§10.2)           | **Cold (local)**  | Immutable, content-addressed checkpoint; branch, merge, reset.               |
| `GitWaypoint` with one or more remotes pushed            | **Cold (remote)** | Offsite redundancy; survives loss of the local machine.                      |

Pure in-memory is hot. A file-backed repository is cold-local. A file-backed repository **with** a remote gives two distinct levels of coldness — local and remote — directly analogous to the redundancy/replication tiers of a cloud object store. Promoting an agent from hot to cold, or adding a second cold tier, is a factory swap and a remote configuration; the lineage, telemetry, and state APIs are unchanged.

### 13.2 Checkpoints are commits

Every waypoint is a checkpoint. With the Git backend it is literally a `RevCommit` (§10.2). This is the role LangGraph's checkpointer plays (§3.1) — but here the checkpoint is content-addressed, carries parents, and supports branch / merge / reset as native operations. As a result, **checkpoint**, **rollback** (§12), and **fork-for-retry** (§11.3) are not three bespoke mechanisms but the same Git primitives viewed from different angles. "Resume the agent from checkpoint X" is `WaypointFactory#root(elementAt(X))` (§9) — root a new traversal at that commit and continue.

### 13.3 Per-resource granularity with EMF

With the EMF backend (§10.3, §10.4) the hot/cold decision is **per resource**, not per agent. A `ResourceSet` holds many `Resource`s, and each can be configured independently — kept in memory only (hot), saved each step (warm), committed (cold-local), or pushed (cold-remote). An agent can therefore keep its scratch and working set hot while durably checkpointing only the resources that matter, all transparently: the agent only ever issues commands against the realm and is unaware which resources are persisted where. This is finer-grained than an all-or-nothing checkpointer, which forces one durability policy on the entire state.

### 13.4 The checkpoint ladder

EMF resources over Git/jGit yield a graded ladder of durability, each rung a distinct, individually addressable checkpoint level:

1. **In-memory mutation** (hottest) — captured by `ChangeRecorder` as a `ChangeDescription`; reversible in-process via inverse-apply (§10.3).
2. **Saved resource** — serialized to the working tree; survives process crash.
3. **Committed change** — a `RevCommit`; immutable, part of history, rollback via `reset` / `revert`.
4. **Pushed change** (coldest) — replicated to one or more remotes; offsite durability.

Recovery selects the appropriate rung rather than always paying for the coldest: inverse-apply a `ChangeDescription` (1), reload a resource (2), reset to a commit (3), or fetch from a remote (4).

### 13.5 Troubleshooting at trace/span level, not log level

Because each `commit` / `merge` is an OpenTelemetry span (§6.5, §10.1) and each step records a `ChangeDescription` (§10.3), agent troubleshooting happens at **trace/span granularity** with change records — and ordinary logs — attached as span events and attributes, rather than by grepping a flat log stream. For any step you have, in one place: the span (rule id, model, token usage, latency, confidence), the exact state delta that step produced (the `ChangeDescription`), and the lineage that led to it (parents). This is strictly finer-grained and more directly actionable than log-level debugging — you can replay or inverse-apply the precise change a step made instead of inferring it from log lines. The waypoint tree *is* the trace tree (§11.1), and each node carries its own reversible diff.

## 14. Asynchronous escalation and human-in-the-loop

A distinctive use of the Git backend (§10.2): **a commit is an escalation.** When an agent reaches a point that needs a human decision — approval, disambiguation, authorization, or a low-confidence rule result (§11.3) — it commits a waypoint whose **tree carries the model state** and whose **commit message carries the execution state of the escalation**: who it escalates to, the deadline, the follow-up cadence, and the action to take on timeout. The commit is the durable, asynchronous hand-off point; the agent process is then free to exit. Resolution arrives later as a further commit, produced either by a human or by an automated timeout action.

This is the human-in-the-loop / interrupt pattern (LangGraph interrupts, Temporal signals — §3.1) realized entirely with Git's native infrastructure — commits, hooks, and server-side actions — rather than a bespoke pause/resume engine.

### 14.1 What goes where

- **Tree (model state).** The waypoint's `S` state, serialized into the repository. When `S` is an EMF model the serialization is trivial and worth doing as **YAML** — human- and machine-readable — so the human reviewing the escalation reads an ordinary Git diff of the model, and the engine reads it back with no custom codec.
- **Commit message (escalation execution state).** The control metadata. Git commit messages have no practical size limit (some forges cap display/storage around 5 MB), so even a verbose escalation prompt with embedded context fits comfortably. Structured fields belong in **Git trailers** (`Key: Value` lines, the same convention as `Co-Authored-By` / `Signed-off-by`), which are trivially parseable while the free-form body stays human-readable:

```
Escalate: target concept ambiguous for source acme:Order

Rule `order-mapping` returned confidence 0.42 (threshold 0.60).
Two candidate target concepts: sales:Order, fulfillment:Shipment.
Please choose, or supply a correction.

Waypoint-Escalation: required
Escalation-To: data-stewards@example.com, alice@example.com
Escalation-Mode: group
Escalation-Deadline: 2026-06-25T17:00:00Z
Escalation-Followup: PT4H
Escalation-On-Timeout: pick-highest-confidence
Waypoint-Id: 3f9a1c…
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

The `traceparent` trailer (§10.2) rides alongside the escalation fields, so the trace survives the cold gap: whoever resolves the escalation — human or timeout job — re-parents its spans under the original trace, and the observability backend shows the whole episode, from the agent's pause to the resolution, as one continuous trace.

### 14.2 The hook / action

A repository hook or forge action fires on the escalation commit and owns the out-of-band lifecycle:

1. **Detect.** Triggered on the commit — a server-side `post-receive` hook or a forge action (GitHub Actions, GitLab webhook) on push. Server-side rather than local `post-commit`, because the escalation and its timers must outlive the agent process. This lands the escalation naturally at the **cold-remote tier (§13)** — where durability and out-of-band integrations belong.
2. **Notify.** Parse the trailers; contact the target(s). `Escalation-Mode: group` fans out to several humans.
3. **Schedule.** Register timer jobs keyed by `Waypoint-Id` — one per `Escalation-Followup` interval (reminders) and one at `Escalation-Deadline` (the timeout action). The commit id makes the jobs idempotent.

### 14.3 Resolution is another commit

Both outcomes converge on the same primitive — a child commit of the escalation waypoint:

- **Human responds.** The decision (chosen concept, correction, approval token) is written as a child commit; the engine resumes by rooting a new traversal there (§9). A group escalation resolves on first authoritative response, or on quorum, with later responses recorded as additional merge parents (§6.1) for the audit trail.
- **Deadline passes.** The timeout job applies `Escalation-On-Timeout` as an automated child commit — e.g. `pick-highest-confidence`, `abort`, or `escalate-to <next>`. The agent's later resumption cannot tell a human resolution from an automated one; both are simply the next waypoint.

Because the unresolved state remains in the graph, escalation inherits the same auditability and retry semantics as everything else (§11.3, §12): nothing is mutated in place; the escalation, its reminders, and its resolution are all durable, content-addressed waypoints.

### 14.4 Assessment

The fit is good and the core needs no changes — escalation is an application pattern over `GitWaypoint` plus conventions, not a new interface. It is worth building as a thin helper in (or alongside) `org.nasdanika.waypoint.git`: an `EscalationTrailers` parser/writer, a reference `post-receive` hook, and a pluggable `Notifier` / `Scheduler` SPI so the timer and messaging backends (cron, a forge's scheduled workflows, a durable queue, email / Slack) are swappable. The one real design caveat is the timer: Git has no native scheduler, so follow-ups and timeouts depend on an external durable scheduler keyed by commit id — that component, not Git, is the source of truth for "has this escalation timed out."

## 15. Case study: budget-bounded proactive construction

This case study composes the primitives above — the lineage DAG (§6), state
threading (§7, §11), guarded state (Core interfaces), durability and
checkpointing (§13), and asynchronous escalation (§14) — into an autonomous
build loop that runs a *commit → evaluate-if-enough-input → act* pattern against
an external model of intent. The worked source of intent is a
[Nasdanika Product Management model](https://product-management.models.nasdanika.org/)
(persona → concern → capability → capability provider), but any typed source of
requirements instantiates `E` the same way.

**Positioning.** The element type `E` is a build step — a
`(Capability, ProviderTemplate)` pair drawn from the intent model. The threaded
state `S` carries two things: the accumulated inputs for the step (requirements,
design, available prerequisites) and a running **budget ledger** (remaining
spend for the period). Because state is read-only from a waypoint's perspective
and transformations produce *new* waypoints (§7), the ledger is auditable by
construction: every act that consumes budget is a commit, and the spend is the
diff between a parent's state and its child's.

**The loop.**

1. **Select — `commit`.** A traversal over the intent graph commits a waypoint at
   the highest-value capability whose provider does not yet exist, prioritized
   persona → concern → capability. An ordinary fast-forward `commit(E next)`.
2. **Evaluate — `map` / `applyAsync`.** A mapper computes readiness from the
   accumulated state: are requirements and design present, are prerequisite
   capabilities Available, is the remaining budget sufficient for the estimated
   cost? Readiness is "the mapper can execute" — no separate guard construct.
3. **Act — guarded `commit`.** If ready, the agent generates the capability
   provider and applies the model edit, the artifact write, and the budget
   decrement through a `RealmWaypoint` so they share one transactional boundary
   (undo recording, invariants). It then commits a child waypoint recording the
   new provider, its provenance, and the spend.
4. **Escalate — async, §14.** If under-specified, the agent does *not* guess. It
   commits a waypoint whose commit message carries the §14 escalation metadata —
   recipient (the persona who holds the unmet concern), the specific input
   needed, a deadline, and a timeout action — and suspends the branch. It resumes
   when a child commit is created: by the human elaborating the design, or by the
   timeout policy (shelve, notify, or proceed on a conservative default).
   "Inform and ask for confirmation" and "request elaboration" are the same
   mechanism with different commit-message payloads.

**Back-pressure and preferences.** The escalation channel of step 4 is rate-
limited by the recipient, not by the loop. Per-persona preferences — opt-out of
proactive building, opt-out of elaboration requests, a maximum request rate
("one per week"), and a cap on concurrent outstanding requests — are part of the
threaded state `S`, so the readiness mapper (step 2) can see them: a candidate
whose only path forward is an escalation the recipient will not accept is *not
ready*, exactly as if a prerequisite were missing. This is reactive-streams
back-pressure applied to human attention — demand is signalled by the consumer,
and the producer (the agent) throttles to match. The §14 timeout closes the
loop on unanswered requests: the escalation waypoint's commit message carries a
deadline and a timeout action, and on expiry the timeout commit releases the
candidate rather than leaving the branch suspended indefinitely. When a branch
is blocked — opted out, throttled, or timed out — the agent does not idle; the
next `select` (step 1) simply commits at the best *reachable* candidate instead.
If Joe's branch is unavailable, Jane's lower-priority capability is built,
because selection ranks by value-per-reachable-step, not by nominal priority
alone. The art of the possible falls out of the geometry: an unreachable branch
contributes no children, so the frontier the loop advances is always the one it
can actually move.

**Unlock as DAG geometry.** A successful act in step 3 changes the readiness of
*downstream* steps — a delivered dependency flips a prerequisite to Available —
so the next selection re-evaluates a frontier the previous commit reshaped. When
Jim's build of provider A (a SQL AST loader) lowers the bar for a capability
whose concern Joe holds, step 2 finds it newly close to ready but
under-specified, and step 4 routes an elaboration request to Joe. The
cross-author handoff is a join (§6 multi-parent waypoints): Joe's elaboration
commit and the agent's suspended branch `merge` into the waypoint from which
construction continues. Conditional flow, retry, and human-in-the-loop are
expressed as waypoint geometry rather than as control constructs — the same
property the NSML case study (§11) relies on.

**Why it is safe to run unattended.** Durability and checkpointing (§13) let the
loop survive process restarts across a month-long budget window — the lineage
*is* the checkpoint, so a crash resumes from the last commit rather than
re-spending. And because the agent traverses an access-controlled view of the
intent model, "what may this agent build, and for whom" is a property of the
produced view, not something the loop must be trusted to enforce.

## 16. Threading and reactive semantics

- **Synchronous methods** (`commit`, `merge`, `map`, `execute`, `apply`, `adapt`) run on the caller's thread. Implementations that need a specific thread (e.g. EMF `EditingDomain` on the UI thread) document this and may block or dispatch.
- **Asynchronous methods** (`*Async`) return a cold `Mono`. Nothing happens until subscription — matching Project Reactor convention.
- **Backpressure:** a single waypoint op is a single-value emission, so `Flux` doesn't appear here. Stream-of-waypoints APIs (walkers, traversal engines) sit on top of this module and may use `Flux`.
- **Sync/async parity:** every operation that can be expressed in both modalities is expressed in both (§5.1). Implementations may share code between them; callers pick at the call site.

## 17. Open questions

1. **Children tracking** — opt-in or always on? Always-on simplifies the API but retains memory for long-running traversals. *Recommendation: opt-in. `getChildren()` returns empty by default; `Waypoints.withChildTracking(factory)` enables it.*
2. **Identity** — `getId()` on the base interface, or only on `GitWaypoint`? *Recommendation: optional `default Optional<String> getId() { return Optional.empty(); }`, overridden where meaningful.*
3. **Equality** — reference equality, or `equals` by parents + element + id? *Recommendation: reference equality only, mirroring jGit (`RevCommit#equals` is identity-by-`ObjectId`).*
4. **Walker / iterator API** — should the module ship a generic `WaypointWalker` analogous to jGit's `RevWalk`? *Probably yes, but in a follow-up.*
5. **Annotations / metadata** — open `Map<String,Object>` for ad-hoc decoration (timing, errors, tags)? *Recommendation: yes, on a small `Annotated` mixin or as a facet, not on the base interface.*
6. **Cancellation** — should `executeAsync` / `mapAsync` propagate cancellation back to in-flight commands when the `Mono` is unsubscribed? *Recommendation: yes, by passing a `reactor.core.publisher.SignalType` listener or using `Disposable` hooks; details TBD per submodule.*
7. **Realm adapt semantics — lens vs snapshot.** `adapt` is lens, `adaptAsync` is snapshot. *Documented in §6.3.1; the NSML-as-adapter use case (§11.6) confirms the choice for the async path — re-running an agentic transformation on every access is wrong. The remaining open question is whether we need a sync-snapshot variant for expensive but deterministic NSML transformations; conceivable, no confirmed call site yet. An async-lens variant has no confirmed use case at all.*

## 18. Alignment with industry conventions — summary

| Concern              | Choice                                                                              |
| -------------------- | ----------------------------------------------------------------------------------- |
| Parent terminology   | `getParents()`, `getParent(int)`, `getParentCount()` — jGit `RevCommit`.            |
| Append verbs         | `commit(E)`, `merge(E, parents)` — Git semantics, not `createChild`.                |
| Collection types     | `List<? extends Waypoint<E>>` for ordered parents; `Collection<? extends Waypoint<E>>` for children. |
| Variance             | `? super` on consumers, `? extends` on producers — `Stream` / `Collectors`.         |
| Iteration parameter  | `Iterable<? extends Waypoint<E>>` — accepts `List`, `Set`, `Collection`, custom.    |
| Varargs ergonomics   | Default method delegating to the `Iterable` overload; warning suppressed once.      |
| Sync/async parity    | One interface, two modalities; async methods suffixed `Async` — Azure OpenAI Java SDK convention. |
| Asynchronous return  | `Mono<T>` / `Mono<Void>` — Project Reactor.                                         |
| Mapper signatures    | `Function<? super X, ? extends Y>` — `Stream#map` convention.                       |
| `AutoCloseable`      | On `TelemetryWaypoint` for try-with-resources span lifecycle.                       |
| Realm idiom          | `execute(Consumer)` / `apply(Function)` — JFace `Realm`, EMF `EditingDomain`, Vert.x `Context#runOnContext`. Closure-capture replaces BiConsumer-style overloads. |

## 19. Backwards compatibility / migration

`Realm`, `ExclusiveRealm`, and `ReadWriteRealm` move from `org.nasdanika.common` to `org.nasdanika.waypoint`. They were experimental, so no stable consumers should exist; nonetheless, the migration is a package rename plus the addition of async methods and `adapt` / `adaptAsync`. Existing implementations need to provide the new methods (mostly mechanical) or extend an abstract base class that supplies sensible defaults.

The existing `org.nasdanika.graph.message` package stays as-is; an adapter in `org.nasdanika.waypoint.graph` will let traversal engines emit waypoints that also produce messages where useful.

## 20. Next steps

1. Create `org.nasdanika.waypoint` module skeleton (Maven coordinates, `module-info.java`, package layout).
2. Move `Realm`, `ExclusiveRealm`, `ReadWriteRealm` from `org.nasdanika.common`, add async methods and `adapt` / `adaptAsync`.
3. Implement the reference `Waypoint<E>` and `StateWaypoint<E, S>` with in-memory, opt-in child tracking.
4. Port the OpGraph execution PoC to `StateWaypoint`.
5. Spike `GitWaypoint` against jGit to validate rollback semantics on a real repository.
6. Spike the NSML integration (§11) against a real mapping — both a deterministic rule and an agent-backed rule — to validate the `RealmStateWaypoint` + telemetry shape end-to-end.
7. Decide §16 open questions — particularly identity, child-tracking default, walker API, and lens vs snapshot semantics for async `adapt` — before stabilising the API.
8. Spike the asynchronous-escalation pattern (§14) — `EscalationTrailers`, a `post-receive` hook, and a `Notifier` / `Scheduler` SPI — against a real low-confidence rule, to validate the commit-as-escalation hand-off end to end.
