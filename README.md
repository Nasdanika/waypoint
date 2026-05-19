# Token Module - Design Document

## 1. Overview

This document specifies a new Nasdanika module - **`org.nasdanika.token`** - providing a generic abstraction for tracking the lineage and payload of an execution as it advances through some structure: a graph, a file system, an EMF model, an AST, a state machine. A `Token` records *where* execution is, *how it got there*, *what data it carries*, and - for richer variants - *which editing domain it runs against* and *which OpenTelemetry span scopes it*.

The model is inspired by three complementary precedents:

- * Workflow/process engine tokens, which carry payload along a process and support rollback / retry through transaction groups.
- **Git** commit history (immutable, parents, fast-forward, merge, reset).
- **Petri-net tokens** and classical workflow semantics.

Originally this was scoped to graph traversal and was going to live alongside `org.nasdanika.graph.message`. On reflection, the abstraction has no inherent dependency on graphs and warrants its own module.

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

A single, structure-agnostic vocabulary - token, commit, merge, state, realm, telemetry - covers all of these, and lets specialized engines (OpGraph runtime, [NSML](https://github.com/Nasdanika-Models/nasdanika-semantic-mapping-language/blob/main/README.md) mapping engine, EMF traversal, file walkers) compose freely with the same primitives.

## 3. Prior art and positioning

The token API draws on - and in places deliberately doesn't replicate - several existing bodies of work. This section surveys the landscape and explains why the module exists as a separate thing rather than building on one of the alternatives.

### 3.1 Survey

**Workflow / process-engine tokens.** - Camunda, Flowable, and Activiti model a running BPMN process as a tree of `Execution` objects: each execution has a parent, a position (an `ActivityImpl`), local variables, and a lifecycle. jBPM predates them. Temporal (ex-Cadence) takes the same shape into a durable, replayable workflow execution - each `WorkflowExecution` has a complete event history that is essentially our token lineage promoted to a persistent first-class artifact. Apache Airflow's task instances are similar, but the DAG structure is rigid.

**AI-era state graphs.** LangGraph's `StateGraph` carries a typed `State` through nodes, supports sync and async node bodies, has a checkpointer (persistent state), interrupts (pause/resume), conditional edges, and parallel branches with state merging. It is essentially `StateToken<Node, State>` with merge semantics, implemented in Python, bound to its own graph structure. Microsoft Semantic Kernel's Process Framework is converging on the same shape. CrewAI and AutoGen carry context less explicitly.

**Observability / context propagation.** Project Reactor's `Context` is the immutable-threaded-state piece, but has no lineage - it's a leaf in your chain, not a tree. Micrometer `Observation` is much closer to `TelemetryToken`: explicit parent/child, scopes, handlers, designed to wrap arbitrary execution. OpenTelemetry's `Span` does most of `TelemetryToken`'s job; our interface is a thin adapter. SLF4J MDC is mutable thread-local with no lineage.

**Functional state threading.** The State monad in Vavr (Java), Cyclops (Java), Arrow (Kotlin), Cats (Scala). `StateToken#map` is a State-monad bind extended with branching. None of these pairs state threading with execution-position tracking or telemetry, because they are language-level rather than domain-level abstractions.

**Content-addressed history.** jGit, Pijul, Mercurial. Event-sourcing frameworks - Axon, EventStoreDB - express the same idea as causation and correlation IDs on events.

**Specifications.** JSR 352 (Java Batch) `StepContext`, checkpoint, restart. JTA savepoints. Older but the rollback semantics map directly.

### 3.2 What we reuse rather than reinvent

- **OpenTelemetry `Span` / `Context`.** `TelemetryToken` is a thin adapter, not a parallel telemetry system. The `org.nasdanika.token.telemetry` submodule is the only place this matters.
- **Micrometer `Observation`.** An alternative `TelemetryToken` backend for Spring-adjacent consumers - adds nothing to the core module.
- **Reactor `Context`.** We *carry* tokens through reactive pipelines using it; we do not replicate it.

### 3.3 Why a separate module rather than adopting an existing system

- **Generic over the position type `E`.** Every existing system fixes the position type - BPMN activity, Airflow task, LangGraph node, Temporal workflow. None lets the consumer say "my position is a `Path`," or "my position is an `EObject` plus the `EReference` we traversed". That is the central novelty, and it's the property that lets one set of primitives serve OpGraph, NSML, EMF traversal, and file walks alike.
- **Composable facets.** State, realm, telemetry as orthogonal mixins. Existing systems either bake everything into a monolithic execution object (Camunda `ExecutionEntity`, Temporal workflow handle) or split telemetry off entirely (OpenTelemetry separate from business logic). Neither makes it easy to choose your set of facets per use case.
- **Library, not runtime.** Camunda, Temporal, LangGraph are runtimes - you cede control to their engine. Token is a library - the engine you build calls into it. This matches OpGraph's positioning: you embed it; it doesn't embed you.

### 3.4 Scope and positioning

The contribution is in the *factoring* - structure-agnostic position, composable facets, reactive-streams-native primitives - not in any single primitive. That is enough to justify the module inside the Nasdanika orbit, where there is concrete downstream demand (OpGraph runtime, NSML execution, EMF traversal, file walks). It is probably *not* enough to justify the module as a standalone library competing for adoption against Micrometer, LangGraph, and Temporal. The published module aims to be useful to anyone who happens to need its shape; it does not chase ecosystem competition.

## 4. Module Layout

```
org.nasdanika.token                       (core - this document)
org.nasdanika.token.telemetry              (OpenTelemetry integration)
org.nasdanika.token.git                    (jGit-backed history persistence)
org.nasdanika.token.emf                    (ResourceSet-backed state, change recording)
org.nasdanika.token.emf.transaction        (EMF Transaction-based RealmToken)
org.nasdanika.token.graph                  (Adapters for org.nasdanika.graph)
org.nasdanika.token.nsml                   (NSML mapping execution - see -11)
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
| Element / position   | (n/a)                      | `getElement()`              | Structural payload - node, file, EObject.                       |
| Append (1 parent)    | `CommitBuilder.commit()`   | `commit(E)`                 | Verb form, mirrors Git fast-forward.                            |
| Append (n parents)   | merge commit               | `merge(E, parents)`         | Same verb as Git merge.                                         |
| Identity             | `getId()` ? `ObjectId`     | `getId()` (optional)        | For correlation, persistence, equality.                         |

The draft's `createChild(...)` is replaced by the two verbs `commit` and `merge`. They carry the Git mental model intact and avoid the noun-y, structure-irrelevant "child".

## 6. Core interfaces

### 6.1 `Token<E>`

```java
public interface Token<E> {

    /** The element this token records execution at. */
    E getElement();

    /**
     * Parents of this token in creation order. Empty list iff this is a
     * root token. Single-element list for linear progressions, multiple
     * elements for joins / merges.
     */
    List<? extends Token<E>> getParents();

    /** Equivalent to {@code getParents().size()}. Matches jGit. */
    default int getParentCount() {
        return getParents().size();
    }

    /** Parent at index {@code n}. Matches jGit. */
    default Token<E> getParent(int n) {
        return getParents().get(n);
    }

    /**
     * Children of this token. May be empty if the implementation does
     * not track children (which is the default - see -14 Open Questions).
     * Unlike Git commits, we expose children because the typical
     * traversal-time use case reads them forward.
     */
    Collection<? extends Token<E>> getChildren();

    /** Single-parent (fast-forward) descendant. */
    Token<E> commit(E next);

    /**
     * Multi-parent (merge) descendant. {@code this} is the first parent;
     * {@code additionalParents} follow in iteration order.
     *
     * @param next               element of the new token
     * @param additionalParents  joining parents, may be empty
     */
    Token<E> merge(E next, Iterable<? extends Token<E>> additionalParents);

    /**
     * Varargs convenience over {@link #merge(Object, Iterable)}.
     * See -7 for the varargs/generics caveat.
     */
    @SuppressWarnings({"unchecked", "varargs"})
    default Token<E> merge(E next, Token<E>... additionalParents) {
        return merge(next, Arrays.asList(additionalParents));
    }
}
```

### 6.2 `StateToken<E, S>`

Adds a state value of type `S`. State is read-only from the token's perspective; transformations produce *new* tokens. This matches reactive-streams / functional style - tokens are values, not mutable cells. (The escape hatch for genuinely mutable state is `RealmToken`, -6.3.)

```java
public interface StateToken<E, S> extends Token<E> {

    /** The state value carried by this token. */
    S getState();

    // -- Narrowed returns --------------------------------------------

    @Override
    List<? extends StateToken<E, ?>> getParents();

    @Override
    Collection<? extends StateToken<E, ?>> getChildren();

    /**
     * Fast-forward that propagates the current state unchanged.
     */
    @Override
    default StateToken<E, S> commit(E next) {
        return commit(next, getState());
    }

    /**
     * Note: Java doesn't allow widening parameter types on override,
     * so the inherited {@code merge(E, Iterable<? extends Token<E>>)}
     * stays with that signature. Implementations check parents are
     * {@code StateToken} at runtime if needed.
     */
    @Override
    StateToken<E, S> merge(E next, Iterable<? extends Token<E>> additionalParents);

    // -- State-aware factories ---------------------------------------

    /** Fast-forward with a new state. */
    <T> StateToken<E, T> commit(E next, T nextState);

    /** Merge with an explicit new state. */
    <T> StateToken<E, T> merge(
            E next,
            T nextState,
            Iterable<? extends StateToken<E, ?>> additionalParents);

    // -- Mapping -----------------------------------------------------

    /**
     * Compute the next state from this token and joining parents,
     * synchronously, on the calling thread.
     *
     * The list passed to the mapper has {@code this} as element 0;
     * joining parents follow in iteration order.
     *
     * @param next                element of the resulting token
     * @param mapper              produces the next state
     * @param additionalParents   joining parents, may be empty
     */
    <T> StateToken<E, T> map(
            E next,
            Function<? super List<? extends StateToken<E, ?>>, ? extends T> mapper,
            Iterable<? extends StateToken<E, ?>> additionalParents);

    /**
     * Asynchronous counterpart of {@link #map}. The mapper returns a
     * {@code Mono<T>}; the result is a {@code Mono<StateToken<E,T>>}.
     * The created token is not visible to observers until the mono
     * emits.
     */
    <T> Mono<StateToken<E, T>> mapAsync(
            E next,
            Function<? super List<? extends StateToken<E, ?>>, ? extends Mono<T>> mapper,
            Iterable<? extends StateToken<E, ?>> additionalParents);
}
```

Notes:

- Mapper signatures use PECS variance (`? super X`, `? extends Y`) - same convention as `Stream#map`, `Stream#collect`, `Collectors`.
- Async returns are `Mono` / `Mono<Void>`, never `CompletionStage`. Adapters can be added later via `Mono#toFuture` / `Mono.fromFuture`.

### 6.3 `RealmToken<E, S>`

For state that *cannot* be propagated by value: an `EditingDomain`, a JDBC `Connection`, a `Repository`, an event-loop `Context`, a JFace `Realm`. Clients don't manipulate the state directly - they submit **commands** that execute against it inside the realm's invariants (synchronization, transactions, undo recording).

The mental model mirrors:

- `org.eclipse.core.databinding.observable.Realm.exec(Runnable)`.
- `EditingDomain.getCommandStack().execute(Command)`.
- `Vertx.runOnContext` / `Context.runOnContext`.

```java
public interface RealmToken<E, S> extends Token<E> {

    @Override
    List<? extends RealmToken<E, S>> getParents();

    @Override
    Collection<? extends RealmToken<E, S>> getChildren();

    @Override
    RealmToken<E, S> commit(E next);

    @Override
    RealmToken<E, S> merge(E next, Iterable<? extends Token<E>> additionalParents);

    /**
     * Execute a command against the realm-managed state synchronously.
     * The token is passed so the command can issue further commits;
     * the state is passed for direct manipulation. Returns when the
     * command has completed.
     */
    void execute(BiConsumer<? super RealmToken<E, S>, ? super S> command);

    /** Async {@link #execute}. */
    Mono<Void> executeAsync(BiConsumer<? super RealmToken<E, S>, ? super S> command);

    /** Compute a value against the realm-managed state. */
    <T> T apply(BiFunction<? super RealmToken<E, S>, ? super S, ? extends T> command);

    /** Async {@link #apply}, flattening the resulting {@code Mono}. */
    <T> Mono<T> applyAsync(
            BiFunction<? super RealmToken<E, S>, ? super S, ? extends Mono<T>> command);
}
```

#### Should `RealmToken` extend `StateToken`?

It has state. But clients aren't expected to call `getState()` directly - they go through `execute` / `apply` to respect the realm's threading and transaction rules. Reading state outside a command can be unsafe.

**Recommendation:** keep them as siblings. Provide a combined interface `RealmStateToken<E, S>` for the cases where direct state read is *also* safe (immutable snapshot state, for instance). This keeps the API honest about thread-safety; clients that genuinely want both opt in by typing against the combined interface.

### 6.4 `TelemetryToken<E>`

Wraps an OpenTelemetry `Span` and `Context`. Each `commit` / `merge` corresponds to a child span; `close()` ends the span. The token is `AutoCloseable` so it works in try-with-resources.

```java
public interface TelemetryToken<E> extends Token<E>, AutoCloseable {

    @Override
    List<? extends TelemetryToken<E>> getParents();

    @Override
    Collection<? extends TelemetryToken<E>> getChildren();

    @Override
    TelemetryToken<E> commit(E next);

    @Override
    TelemetryToken<E> merge(E next, Iterable<? extends Token<E>> additionalParents);

    /**
     * OpenTelemetry context for propagation across process boundaries
     * (HTTP headers, message attributes).
     */
    Context getContext();

    /**
     * Tracer for instrumenting code that wants to create spans
     * beneath this token's span without creating a child token.
     */
    Tracer getTracer();

    /** Optional meter, for tokens that also scope metrics. */
    Meter getMeter();

    /**
     * End the span associated with this token. Idempotent.
     * Does not close children's spans.
     */
    @Override
    void close();

    /** Async close - when the span lifecycle ends with an async op. */
    Mono<Void> closeAsync();

    /** Run a command with this token's context activated. */
    void execute(Consumer<? super TelemetryToken<E>> command);

    Mono<Void> executeAsync(Consumer<? super TelemetryToken<E>> command);

    <T> T apply(Function<? super TelemetryToken<E>, ? extends T> command);

    <T> Mono<T> applyAsync(
            Function<? super TelemetryToken<E>, ? extends Mono<T>> command);
}
```

The `applyAsync` mapper returns `Mono<T>` and the method returns the flattened `Mono<T>` - the same shape as `Mono#flatMap`, avoiding an awkward `Mono<Mono<T>>`.

## 7. Generic varargs

The draft poses: *"will the compiler complain about generic varargs?"* Yes. `Token<E>... parents` produces an *"unchecked generic array creation for varargs parameter"* warning at every call site. `@SafeVarargs` requires the annotated method to be `final`, `static`, or `private` - none of which applies to a regular interface method. It *can* be applied to a `private` interface method in Java 9+, but not to a `default` one.

Options considered, in order of preference:

1. **Drop the varargs from the interface entirely.** Only declare the `Iterable` overload. Callers use `List.of(a, b)` or `Set.of(a, b)`. Cleanest, most reactive-streams-style.
2. **Keep a `default` varargs convenience method** that delegates to the `Iterable` overload, with `@SuppressWarnings({"unchecked","varargs"})` on the declaration. Call sites stay clean.
3. **Static varargs factory in a `Tokens` utility class** that *is* `final` and *can* legally bear `@SafeVarargs`.

**Recommendation:** option 2 - pragmatic, idiomatic with `List.of` / `Stream.of`, and the warning is suppressed exactly once at the declaration site.

## 8. Combinations of facets

We need tokens that are both telemetric and stateful, or both telemetric and realm-bound. Two strategies, ship both.

### 8.1 Composed interfaces (typed)

```java
public interface TelemetryStateToken<E, S>
        extends TelemetryToken<E>, StateToken<E, S> {

    @Override
    List<? extends TelemetryStateToken<E, ?>> getParents();

    @Override
    TelemetryStateToken<E, S> commit(E next);
    // ...narrowed returns
}

public interface TelemetryRealmToken<E, S>
        extends TelemetryToken<E>, RealmToken<E, S> { /* ... */ }

public interface RealmStateToken<E, S>
        extends RealmToken<E, S>, StateToken<E, S> { /* ... */ }
```

Pro: single object, strong typing at call sites.
Con: combinatorial growth when facets multiply (auth context, MDC, transaction handle, -). Bridge methods are needed to narrow return types, and default methods can clash between super-interfaces.

### 8.2 Facet pattern (open-ended)

```java
public interface Token<E> {
    // ...
    default <F> Optional<F> facet(Class<F> facetType) {
        return Optional.empty();
    }
}

// Usage:
token.facet(TelemetryFacet.class)
     .ifPresent(t -> t.span().addEvent("..."));
token.facet(StateFacet.class)
     .ifPresent(s -> useState(s.value()));
```

Pro: open-ended; aligns with Eclipse `IAdaptable` and EMF `EObject.eAdapters()` - both already used by Nasdanika.
Con: less type-safe; needs explicit lookup.

**Recommendation:** ship both. Composed interfaces cover the common cases with strong typing; `facet(...)` keeps `Token<E>` future-proof.

## 9. Token factories

A `TokenFactory<E, T>` produces the root token(s) and encapsulates traversal-wide policy (child tracking on/off, threading model, telemetry hookups, -).

```java
public interface TokenFactory<E, T extends Token<E>> {

    /** New root with no parents. */
    T root(E element);

    /**
     * New root with the given parents. Useful for resuming a traversal
     * or splicing sub-traversals together.
     */
    T root(E element, Iterable<? extends T> parents);
}

public interface StateTokenFactory<E, S>
        extends TokenFactory<E, StateToken<E, S>> {

    StateToken<E, S> root(E element, S initialState);
}

public interface RealmTokenFactory<E, S>
        extends TokenFactory<E, RealmToken<E, S>> { /* ... */ }

public interface TelemetryTokenFactory<E>
        extends TokenFactory<E, TelemetryToken<E>> {

    TelemetryToken<E> root(E element, Tracer tracer);
}
```

A `Tokens` utility class hosts decorators and static helpers, and is the right place for `@SafeVarargs` static varargs methods (see -7):

```java
public final class Tokens {
    private Tokens() {}

    public static <E> TokenFactory<E, Token<E>> recording();

    public static <E, S> StateTokenFactory<E, S> stateful();

    public static <E, T extends Token<E>> TokenFactory<E, T> withChildTracking(
            TokenFactory<E, T> delegate);

    @SafeVarargs
    public static <E> List<Token<E>> parents(Token<E>... parents) {
        return List.of(parents);
    }
}
```

## 10. Technology-specific submodules

### 10.1 `org.nasdanika.token.telemetry`

Reference `TelemetryToken` implementation backed by the OpenTelemetry SDK:

- Span creation per `commit` / `merge`. Single-parent ? `Span.setParent`; multi-parent ? primary parent via `setParent`, joining parents via `Span.addLink`.
- Context propagation across process boundaries via `TextMapPropagator`.
- Optional `Meter` for metrics scoped to the span.

### 10.2 `org.nasdanika.token.git`

`GitToken<E, S>` persists each commit to a Git repository - element + state are serialized to blobs and recorded as a real `RevCommit`. Branches and merges happen at the jGit level. Use cases: durable execution history for long-running or replayable processes, rollback via `git reset` / `git revert`, forensic analysis.

```java
public interface GitToken<E, S> extends StateToken<E, S> {
    ObjectId getCommitId();
    RevCommit getCommit();
    Ref getBranch();
}

public final class GitTokenFactory<E, S>
        implements StateTokenFactory<E, S> {

    public GitTokenFactory(
            Repository repository,
            String branch,
            Serializer<E> elementSerializer,
            Serializer<S> stateSerializer) { /* ... */ }
}
```

### 10.3 `org.nasdanika.token.emf`

Bridges to EMF `ResourceSet` / `Resource`:

- `ResourceSetStateToken<E>` - `S = ResourceSet`; child tokens see snapshots via copying.
- `ChangeRecordingStateToken<E>` - wraps `org.eclipse.emf.ecore.change.util.ChangeRecorder`; each step records a `ChangeDescription` that can be applied forward or applied-inverse (rollback).

### 10.4 `org.nasdanika.token.emf.transaction`

Bridges to `org.eclipse.emf.transaction`. Commands issued via `RealmToken#execute` are wrapped in `RecordingCommand`s and pushed to the editing domain's command stack, so EMF undo/redo and write-locks work transparently.

### 10.5 `org.nasdanika.token.graph`

Adapters for `org.nasdanika.graph`. Generic `Element` (node/connection) is the natural `E`. This is the original OpGraph use case; it lives here rather than in core so the core module has no graph dependency.

## 11. Mapping and transformation case study

A worked example tying the module together, and the reason `org.nasdanika.token.nsml` (and an analogous integration into NSML itself) is on the roadmap. 

NSML - the Nasdanika Semantic Mapping Language - describes transformations from source models to target/semantic models via rules. An execution of an NSML mapping is a traversal of the source structure that produces target structure. Each rule firing is an execution step. The fit with tokens is direct.

### 11.1 Mapping execution as tokens

**Position (`E`).** A `MappingStep` value combining the source element being mapped and the rule that fired:

```java
record MappingStep(EObject source, MappingRule rule) {}
```

`E = MappingStep` gives the lineage immediate meaning - every token says "rule R was applied to source S."

**State (`S`).** A `MappingContext` holding:

- The target `ResourceSet` being built.
- A bindings map: source `EObject` ? target `EObject`(s).
- Accumulated diagnostics (errors, warnings, low-confidence flags from AI rules).
- Source-tracing references for downstream provenance queries.

In practice the mapping context is realm-managed - writes to the target `ResourceSet` go through commands - so `RealmStateToken<MappingStep, MappingContext>` is the right shape. Reading the bindings map for routing decisions is safe; writes go through `execute`.

**Telemetry.** Each `commit` / `merge` is a span. Rule attributes go on the span: `nsml.rule.id`, `nsml.rule.confidence`, `nsml.source.uri`. Rule bodies that call AI agents emit child spans with OpenTelemetry gen-ai semantic-convention attributes (`gen_ai.system`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`). The token tree *is* the trace tree.

**Lineage as provenance.** "Why does this target element exist?" is answered by walking parents from the token that produced it. The answer is structurally available, not reconstructed from logs. Every target element carries a reference (an EAnnotation, an EMF Adapter, or a side index) to the token whose rule firing produced it.

### 11.2 Worked sketch

```java
// Engine startup
var factory = new NsmlTokenFactory(targetResourceSet, openTelemetry);
RealmStateToken<MappingStep, MappingContext> root = factory.root(
        new MappingStep(null, null),       // synthetic root step
        MappingContext.fresh());

// A deterministic rule firing
var step = new MappingStep(sourceEObject, rule);
RealmStateToken<MappingStep, MappingContext> child = root.commit(step);
child.execute((token, ctx) -> {
    var targetEObject = rule.apply(sourceEObject, ctx);
    ctx.bind(sourceEObject, targetEObject);
    ctx.annotateProvenance(targetEObject, token);
});

// An AI-backed rule
RealmStateToken<MappingStep, MappingContext> agentStep =
        child.commit(new MappingStep(otherSource, agentRule));
agentStep.applyAsync((token, ctx) -> agent
        .chooseTargetConcept(otherSource, ctx)
        .doOnNext(answer -> ctx.recordConfidence(token, answer.confidence()))
        .map(answer -> answer.targetConcept()))
    .subscribe(/* consumer */);

// Retry on low confidence - sibling under the same parent, audit trail intact
var lastAnswer = agentStep.<Answer>apply((t, ctx) -> ctx.lastAnswer(t));
if (lastAnswer.confidence() < 0.6) {
    var retryStep = child.commit(
            new MappingStep(otherSource, agentRule.withHigherTemperature()));
    // ...retry on the new branch; old branch retained for audit
}
```

### 11.3 Why async + AI agents fit naturally

Three properties of the token API matter for agentic rules, and all of them come from the core design rather than from special-purpose orchestration:

1. **Real asynchrony with backpressure.** `applyAsync` returns a cold `Mono`; agents are I/O-bound; fan-out (`Flux.merge`, `Flux.concatMap`) is governed by Reactor's standard backpressure mechanics.
2. **Retry from a known-good state.** `pred.commit(retryStep)` snaps back to a known-good token. Retries with different prompts, temperatures, or models become siblings under the same parent. Nothing is lost from the audit trail - the rejected attempt remains in the token tree.
3. **Observable cost and latency.** Per-step OTEL spans capture model, token usage, latency, cost. Aggregated across a mapping run, you get per-rule and total AI cost of producing the target model - which matters in production.

### 11.4 Declarative and agentic rules - uniform shape

A deterministic NSML rule ("if source is `UMLClass`, create `OOPClass`") and an agentic rule ("ask the model to choose the best target concept given source and surrounding context") produce identical execution records - same lineage, same telemetry shape, same retry semantics. The only difference is in the rule body. That's a clean factoring you don't get if agent invocation is built into a special-purpose orchestrator: declarative and agentic transformations are no longer different kinds of system, just different rule bodies.

### 11.5 Submodule sketch

```
org.nasdanika.token.nsml
+-- NsmlToken                       // alias for RealmStateToken<MappingStep, MappingContext>
+-- NsmlTokenFactory                // root tokens, engine wiring, OTEL hookup
+-- MappingStep                     // (sourceEObject, rule)
+-- MappingContext                  // target ResourceSet + bindings + diagnostics
+-- ProvenanceAnnotator             // links target elements to the producing token
+-- agent
    +-- AgentRule                    // base for AI-backed rules
    +-- GenAiSpanAttributes          // OTEL gen-ai semantic-convention helpers
```

## 12. Rollback and retry

The token graph itself is append-only; rollback is achieved by:

1. Locate a token at the desired pre-failure point (`pred`).
2. Create a new child of `pred` for the retry: `pred.commit(retryElement)`.
3. For `StateToken`, `pred.getState()` is the starting state for the retry.
4. For `GitToken`, this maps to `git reset --hard <pred-commit>` and continuing.
5. For `ChangeRecordingStateToken`, inverse-apply the `ChangeDescription`s back to `pred` before retrying.

Failure handling is deliberately *not* part of the core `Token<E>` interface - different traversal engines have different failure semantics (best-effort, transactional, compensating). They build on top of these primitives.

## 13. Threading and reactive semantics

- **Synchronous methods** (`commit`, `merge`, `map`, `execute`, `apply`) run on the caller's thread. Implementations that need a specific thread (e.g. EMF `EditingDomain` on the UI thread) document this and may block or dispatch.
- **Asynchronous methods** (`*Async`, the `mapAsync` family) return a cold `Mono`. Nothing happens until subscription - matching Project Reactor convention.
- **Backpressure:** a single token op is a single-value emission, so `Flux` doesn't appear here. Stream-of-tokens APIs (walkers, traversal engines) sit on top of this module and may use `Flux`.

## 14. Open questions

1. **Children tracking** - opt-in or always on? Always-on simplifies the API but retains memory for long-running traversals. *Recommendation: opt-in. `getChildren()` returns empty by default; `Tokens.withChildTracking(factory)` enables it.*
2. **Identity** - `getId()` on the base interface, or only on `GitToken`? *Recommendation: optional `default Optional<String> getId() { return Optional.empty(); }`, overridden where meaningful.*
3. **Equality** - reference equality, or `equals` by parents + element + id? *Recommendation: reference equality only, mirroring jGit (`RevCommit#equals` is identity-by-`ObjectId`).*
4. **Walker / iterator API** - should the module ship a generic `TokenWalker` analogous to jGit's `RevWalk`? *Probably yes, but in a follow-up.*
5. **Annotations / metadata** - open `Map<String,Object>` for ad-hoc decoration (timing, errors, tags)? *Recommendation: yes, on a small `Annotated` mixin or as a facet, not on the base interface.*
6. **Cancellation** - should `executeAsync` / `mapAsync` propagate cancellation back to in-flight commands when the `Mono` is unsubscribed? *Recommendation: yes, by passing a `reactor.core.publisher.SignalType` listener or using `Disposable` hooks; details TBD per submodule.*

## 15. Alignment with industry conventions - summary

| Concern              | Choice                                                                              |
| -------------------- | ----------------------------------------------------------------------------------- |
| Parent terminology   | `getParents()`, `getParent(int)`, `getParentCount()` - jGit `RevCommit`.            |
| Append verbs         | `commit(E)`, `merge(E, parents)` - Git semantics, not `createChild`.                |
| Collection types     | `List<? extends Token<E>>` for ordered parents; `Collection<? extends Token<E>>` for children. |
| Variance             | `? super` on consumers, `? extends` on producers - `Stream` / `Collectors`.         |
| Iteration parameter  | `Iterable<? extends Token<E>>` - accepts `List`, `Set`, `Collection`, custom.       |
| Varargs ergonomics   | Default method delegating to the `Iterable` overload; warning suppressed once.      |
| Asynchronous return  | `Mono<T>` / `Mono<Void>` - Project Reactor.                                         |
| Mapper signatures    | `Function<? super X, ? extends Y>` - `Stream#map` convention.                       |
| `AutoCloseable`      | On `TelemetryToken` for try-with-resources span lifecycle.                          |
| Editing-domain idiom | `execute(BiConsumer)` / `apply(BiFunction)` - JFace `Realm`, EMF `EditingDomain`, Vert.x `Context#runOnContext`. |

## 16. Backwards compatibility / migration

Greenfield - nothing to migrate. The existing `org.nasdanika.graph.message` package stays as-is; an adapter in `org.nasdanika.token.graph` will let traversal engines emit tokens that also produce messages where useful.

## 17. Next steps

1. Create `org.nasdanika.token` module skeleton (Maven coordinates, `module-info.java`, package layout).
2. Implement the reference `Token<E>` and `StateToken<E, S>` with in-memory, opt-in child tracking.
3. Port the OpGraph execution PoC to `StateToken`.
4. Spike `GitToken` against jGit to validate rollback semantics on a real repository.
5. Spike the NSML integration (-11) against a real mapping - both a deterministic rule and an agent-backed rule - to validate the `RealmStateToken` + telemetry shape end-to-end.
6. Decide -14 open questions - particularly identity, child-tracking default, and walker API - before stabilising the API.
