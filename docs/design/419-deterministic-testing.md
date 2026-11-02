# Deterministic and model-based testing for distributed-process

Design notes for [issue #419](https://github.com/haskell-distributed/distributed-process/issues/419).

Status: proposal, for discussion. Nothing here is implemented.

## 1. Summary

The proposal in #419 is a test-only in-memory transport that lets a test rig
deliberately manipulate the network. That is the right first step, and this
document sets out what it should look like. It also answers the question raised
in the thread about whether GHC offers, or will offer, a replaceable scheduler
in the way Erlang does for PULSE.

The short answers:

- GHC has no pluggable scheduler and no proposal to add one. Do not wait for it.
- Two mature libraries already solve the general problem, `dejafu` and `io-sim`,
  and both require the code under test to be polymorphic in its base monad.
  That is the blocker LaurentRDC identified, and it is a real one.
- The structure of a `LocalNode` is more favourable than it looks. Almost all
  per-node activity is already funnelled through one single-threaded loop. A
  test rig that controls the transport and that one loop gets most of the
  determinism that matters without touching the base monad.
- GHC 9.6 added thread introspection (`GHC.Conc.listThreads`, `threadLabel`),
  which gives us quiescence detection. That replaces the `threadDelay` calls
  the current suite relies on, and it needs no architectural change at all.

The recommendation is a layered plan where each layer is independently useful
and the expensive monad abstraction is deferred until we have evidence we need
it.

## 2. What already exists for GHC Haskell

### 2.1 Replacing the scheduler: not available

GHC's RTS scheduler is round-robin in `rts/Schedule.c`, with no user-pluggable
interface. The serious attempt to change that was "Composable Scheduler
Activations for Haskell" (Sivaramakrishnan, Harris, Marlow, Peyton Jones),
which added RTS upcalls so that user-level schedulers could be written as
Haskell libraries. It lived on a GHC fork, it is roughly a decade old, and it
was never upstreamed. GHC issue #7606 (stride scheduling) is about thread
priorities and fairness rather than test control, and is similarly old. There
is no ghc-proposals entry for scheduler hooks.

The one relevant new primitive is delimited continuations, `prompt#` and
`control0#`, from [proposal
#313](https://ghc-proposals.readthedocs.io/en/latest/proposals/0313-delimited-continuation-primops.html),
implemented in GHC 9.6. These give the mechanism a cooperative user-level
scheduler needs, namely capturing and restoring a continuation in `IO` without
CPS-transforming the program. The proposal itself is blunt about the caveats:
`control0#` is dangerous with arbitrary `IO` code because existing code does
not anticipate re-entrant `IO`, async exceptions must be unmasked during
capture and re-masked on restore, it only works with `RealWorld`, and it cannot
capture across an FFI boundary.

More importantly, delimited continuations give us the mechanism but not the
interception. They do not let us intercept `MVar`, `STM`, or `forkIO`
operations in code we did not write. The code under test still has to call our
primitives. So they help build section 5.2 below, and nothing else here.

### 2.2 `dejafu` + `concurrency`

Michael Walker's `dejafu` provides `MonadConc` and `MonadSTM` typeclasses over
GHC's concurrency primitives, plus systematic exploration using schedule
bounding and dynamic partial order reduction, with selectable memory models
(SC, TSO, PSO) for unsynchronised `IORef` access. Version 2.4.0.8 was uploaded
in January 2026, so the package is alive, and the project documents a policy of
supporting at least the latest four GHC releases.

Requirement: code under test must be written as `MonadConc m => m a`.

### 2.3 `io-sim` + `io-classes`

The Well-Typed and IOG stack. `io-classes` is the typeclass layer
(`MonadSTM`, `MonadAsync`, `MonadFork`, `MonadThrow`, `MonadTimer`,
`MonadTime`, plus `strict-stm`, `strict-mvar`, and `si-timers` sublibraries).
`io-sim` provides two interpreters:

- `IOSim`, a pure discrete-event simulator with a virtual clock and a full
  execution trace. Deterministic, so a failing seed always reproduces, but it
  explores one schedule.
- `IOSimPOR`, which dynamically detects races within a time slice, then runs
  schedules that reverse the order of racing events and recursively discovers
  and reverts new races, bounded by a partial-order-reduction algorithm.

`IOSimPOR` was developed with John Hughes of QuviQ, so it is the direct
descendant of the PULSE work referenced in the issue. IOG used this to test the
whole Cardano diffusion layer, including a simulated network
(`Simulation.Network.Snocket`) that can inject network errors, model delays,
and reproduce TCP simultaneous open. That is very close to what #419 asks for,
built on top of the monad abstraction rather than on a bespoke transport.

Requirement: code under test must be polymorphic over the `io-classes`
constraints. Note also that `io-classes` 1.11 declares `base >= 4.16` and is
tested with GHC 9.6 through 9.14, whereas this repository's `tested-with` floor
is GHC 8.10.7.

### 2.4 `stevana/deterministic-scheduler`

Stevan Andjelkovic's port of matklad's approach, and the most useful thing to
read before building anything here, because it is small and it keeps the base
monad at `IO`. The core is a per-thread signal:

```haskell
data Signal = SingleThreaded | MultiThreaded (TMVar ())
```

`SingleThreaded` makes pausing a no-op, so production and single-threaded runs
pay nothing. A managed thread calls `pause`, which takes from the `TMVar` and
blocks until the scheduler puts a unit back. The scheduler calls
`waitUntilAllPaused` (one STM transaction over all threads), picks a thread
with `randomR` from a seeded generator, unpauses it, and waits for it to pause
again or finish. Shared state is reached through a swappable record, where the
fake inserts a `pause` before and after each operation.

Documented limitations: it is white-box, so the system under test must be
modified. Only random interleavings are sampled, with no exhaustive enumeration
or partial order reduction. Shrinking perturbs the interleaving, so some seeds
do not yield a minimal counterexample. The signal is threaded through a
`ReaderT Signal IO`, and QuickCheck's own seed is reused as the scheduler seed.

### 2.5 Model-based testing libraries

| Library | Version | Sequential | Parallel / linearisability | Notes |
| --- | --- | --- | --- | --- |
| `quickcheck-state-machine` | 0.10.x | yes | yes, built in | Variable types at the type level, which tends to produce awkward type errors |
| `quickcheck-dynamic` | 4.x | yes | no | Adds dynamic logic for writing targeted scenario properties |
| `quickcheck-lockstep` | 0.8.x (Jul 2025, GHC 9.8-9.12) | yes | no | Built on `quickcheck-dynamic`, real-versus-model response comparison, the most approachable of the three |

### 2.6 Options that need no code change at all

- `rr record --chaos` randomises thread priorities across recording runs and
  replays deterministically. Useful for turning a Heisenbug into a reproducible
  one. Caveat: `rr` serialises execution onto one core, which masks some races,
  and there are reported cases of races that reproduce natively but not under
  chaos mode.
- `+RTS -C0` sets the context switch interval so that a switch happens at every
  heap block allocation rather than on the default 20ms timer. The cheapest
  available way to perturb scheduling. Worth running the existing suite under
  `-C0` with both `-N1` and `-N4`.

### 2.7 The GHC feature that helps this codebase most

`GHC.Conc.listThreads :: IO [ThreadId]` and `threadLabel`, both since
`base-4.18` (GHC 9.6). Combined with `threadStatus`, a test rig can enumerate
every live thread and ask whether each is `ThreadBlocked BlockedOnMVar`,
`BlockedOnSTM`, `ThreadFinished`, and so on.

That gives us quiescence detection without rewriting the library: step one
event, wait until every thread is blocked or finished, then step the next. It
converts "sleep 100ms and hope" into "advance once the system has actually
settled".

One caveat: under `-threaded`, `threadDelay` and blocking I/O report as
`BlockedOnMVar`, so thread status alone cannot distinguish "waiting on a timer"
from "waiting on a lock". Quiescence detection is therefore only sound once
real time is removed from the system under test, which is why section 5.1
depends on the rig owning the clock.

## 3. Where the nondeterminism actually is

Reading the current implementation, each `LocalNode` runs exactly two
infrastructure threads:

- `handleIncomingMessages` (`Node.hs:513`) drains `NT.receive endpoint` in a
  loop. Every inbound transport event on the node passes through this one
  point, in order.
- `runNodeController` (`Node.hs:655`) is a single-threaded state machine
  reading `NCMsg` values from `localCtrlChan :: Chan NCMsg`.

Crucially, nearly everything funnels through the node controller. `send` to a
local pid goes to `sendLocal`, which is
`sendCtrlMsg Nothing (LocalSend to msg)`, which is a `writeChan ctrlChan`.
The same is true of `link`, `monitor`, `unlink`, `unmonitor`, `exit`, `kill`,
`register`, `whereis`, `spawn`, and `getInfo`. The only paths that bypass the
node controller are remote `Received` payloads, which `handleIncomingMessages`
enqueues straight into a process's `CQueue` (the `ToProc` branch) or a typed
channel's `TQueue` (the `ToChan` branch), and `usend` via `UnreliableSend`.

So a process mailbox has exactly two writers, the node controller thread and
the incoming-message thread. The control channel has many writers, namely every
local process plus the incoming-message thread.

That gives a complete nondeterminism budget:

| | Source | Controlled by |
| --- | --- | --- |
| N1 | Transport event order across endpoints and connections | Layer 1, the sim transport |
| N2 | `ctrlChan` admission order among concurrent local writers | Layer 2, the control plane |
| N3 | Node controller versus incoming handler racing to write one mailbox | Layer 2 |
| N4 | When each user process thread runs between its own calls | Layer 3 |
| N5 | Real time: `threadDelay`, `receiveTimeout`, `after`, the RTS tick | Layer 0 and Layer 3 |

In the current in-memory transport, N1 arises because `connect`, `send`, and
`close` each write `ConnectionOpened`, `Received`, or `ConnectionClosed`
directly into the remote endpoint's `TChan` inside the sender's own STM
transaction (`Network/Transport/InMemory/Internal.hs`). The resulting order is
STM commit order, which no test can influence.

## 4. Layer 0: make the existing suite honest

Independent of everything else, and worth doing first.

1. Add quiescence helpers to `distributed-process-systest`, built on
   `listThreads` and `threadStatus`, behind a `base >= 4.18` CPP guard:

   ```haskell
   awaitQuiescence  :: IO ()              -- all threads blocked or finished
   awaitQuiescenceOf :: [ThreadId] -> IO ()
   ```

   Then replace the `threadDelay` calls that exist only to let messages
   settle. There are 13 in `Tests/CH.hs`, 4 in `Tests/Closure.hs`, and 2 each
   in `Tests/Mx.hs` and `Tests/Tracing.hs`.

2. Label every infrastructure thread with `labelThread`: the node controller,
   the incoming handler, and each process. This makes quiescence reports and
   `Mx` traces readable, and it is useful on its own.

3. Add `+RTS -C0` variants to CI, with `-N1` and `-N4`. Free bug-finding before
   any of the work below.

Success criterion for this layer: `distributed-process-tests` no longer needs
`localOption (1 :: NumThreads)`. The comment in `tests/runInMemory.hs` says
tests are run sequentially because concurrent execution slows them enough that
`threadDelay`-based synchronisation wakes up too late. Removing that constraint
is a concrete, measurable goal.

## 5. Layer 1: `network-transport-sim`

This is the deliverable #419 asked for.

Put it in a new package rather than extending `network-transport-inmemory`, so
the production transport stays simple and the test transport is free to
diverge. It should depend only on `network-transport`.

The one structural change from the in-memory transport is that delivery becomes
two-phase. Instead of writing into the remote endpoint's `TChan` inside the
sender's transaction, `connect`, `send`, and `close` append to an ordered
per-connection in-flight queue with a sequence number. A separate delivery step
moves events from in-flight queues into the destination endpoint's inbox, and
the rig decides which step happens next.

```haskell
data SimTransport = SimTransport
  { simTransport :: Transport
  , simControl   :: SimControl
  }

data SimControl = SimControl
  { -- inspection
    inFlight    :: IO [Pending]
    -- stepping
  , deliverNext :: IO (Maybe Delivered)         -- oldest eligible event
  , deliverOne  :: PendingId -> IO Bool         -- a specific event, to reorder
  , drain       :: IO ()                        -- deliver until nothing is pending
    -- faults
  , partition   :: Set EndPointAddress -> Set EndPointAddress -> IO ()
  , heal        :: IO ()
  , dropOne     :: PendingId -> IO Bool
  , duplicate   :: PendingId -> IO ()
  , crashEndPoint :: EndPointAddress -> IO ()
  , breakLink   :: EndPointAddress -> EndPointAddress -> IO ()
    -- policy
  , setPolicy   :: DeliveryPolicy -> IO ()
  }

data DeliveryPolicy
  = Immediate              -- today's behaviour, for drop-in compatibility
  | Manual                 -- nothing moves unless the rig says so
  | Randomised StdGen      -- seed-driven and reproducible
```

### 5.1 Invariants the rig must preserve

`distributed-process` depends on these, so `deliver` has to enforce them rather
than trust the test author.

- `ReliableOrdered` means per-connection FIFO. The rig may reorder across
  connections and across endpoints freely, but not within a connection, and
  `ConnectionOpened` must precede any `Received` on that connection, with
  `ConnectionClosed` last. Model this as eligibility: a pending event is
  eligible only when every earlier pending event on the same connection has
  been delivered. `deliverNext` picks from the eligible set.
- A netsplit must raise
  `ErrorEvent (TransportError (EventConnectionLost addr) _)` on both sides,
  exactly as `apiBreakConnection` does today, or the `node_disconnect` rule in
  `handleIncomingMessages` will not fire. Reuse that logic rather than
  reimplementing it.
- Asymmetric partitions matter and today's `breakConnection` cannot express
  them, because it breaks both directions at once. `partition` should take a
  directed edge set. One-way partitions are a historically under-tested failure
  class.

### 5.2 Deliberate misbehaviour

The transport's obligations are documented in the Unified semantics, but
nothing tests what happens when a transport violates them. The sim transport
should be able to deliver `Received` before `ConnectionOpened`, deliver a
duplicate, and deliver `ConnectionClosed` twice. `distributed-process` should
fail loudly rather than corrupt state, and at present nobody knows which it
does. This is worth a small test group of its own.

### 5.3 Integration

Extend `TestTransport` in `distributed-process-tests`:

```haskell
data TestTransport = TestTransport
  { testTransport       :: NT.Transport
  , testBreakConnection :: NT.EndPointAddress -> NT.EndPointAddress -> IO ()
  , testSimControl      :: Maybe SimControl    -- new
  }
```

Existing tests are unaffected, and new tests pattern-match on `testSimControl`
and skip when it is `Nothing`. One suite continues to run against TCP, QUIC,
in-memory, and sim.

### 5.4 What this layer buys

N1 goes away. Netsplits, asymmetric partitions, message loss, duplication,
cross-connection reordering, slow links, and endpoint crashes all become
scripted and reproducible with no sleeps. It does not address N2 through N5:
deliveries between nodes are ordered, but what each node's processes do between
deliveries is still at the GHC scheduler's discretion.

## 6. Layer 2: a node-controller control plane

The instinct in the issue that a patched node controller is also needed is
correct, and the cost is lower than it looks, because the node controller is
already a single-threaded loop over one channel.

Rather than forking the node controller, make its two loops steppable behind a
policy chosen at node construction. Replace `localCtrlChan :: Chan NCMsg` with
an abstract sink:

```haskell
data CtrlQueue = CtrlQueue
  { ctrlEnqueue :: NCMsg -> IO ()
  , ctrlDequeue :: IO NCMsg
  }
```

In production this is `CtrlQueue (writeChan c) (readChan c)`, which is
behaviourally identical and adds one indirection on a path that already
allocates. Under test, the enqueue side appends to a rig-visible ordered buffer
tagged with the calling thread's label, and the dequeue side blocks until the
rig releases an entry. The rig then sees and orders every `Link`, `Monitor`,
`Unlink`, `Unmonitor`, `Exit`, `Kill`, `LocalSend`, `Died`, and `Register`,
which is where the interesting races are: `Died` arriving before `Monitor` is
processed, `Unlink` racing `Died`, `Register` racing a node disconnect.

A second, smaller hook wraps the mailbox write so the rig can order node
controller versus incoming handler writes (N3), and so a chaos monkey can drop
a delivery.

To keep this non-invasive, add a `newLocalNodeWith :: NodeHooks -> ...`
alongside the existing `newLocalNode`, with an all-defaults `NodeHooks` record
living in an `Internal` module carrying no stability guarantee. That is already
the convention here, as with `TransportInternals` and
`createTransportExposeInternals`.

This also makes the chaos monkey from the original issue tractable and targeted
rather than random. The rig already knows every pid through the node controller
state, and the `Mx` layer already publishes process lifecycle events. The open
question from 2013 about how to identify supervisors has a better answer than
inference: mark system processes at spawn time with a flag in `LocalProcess`,
and let the rig filter on it.

## 7. Layer 3: the scheduler

Three options, in increasing cost. The recommendation is to do 7.1 and then
stop and reassess.

### 7.1 Quiescence stepping

Combine Layer 1, Layer 2, and `listThreads`-based quiescence. The rig's loop
is: release exactly one event, either a transport delivery or a control
message, then wait for global quiescence, then release the next.

Between releases the GHC scheduler runs whatever it likes, but because only one
event was injected and the system then runs to a fixpoint, the observable
interleaving is determined by the rig's choice sequence. This holds as long as
rig-controlled events are the only source of progress, which means real time
must be banned in the system under test (see the caveat in section 2.7).

This does not give full interleaving coverage. It will not find a race between
two user process threads that are both already runnable. It does make the
entire protocol layer deterministic, which is where this library's bugs
actually live: monitor and link ordering, exit propagation, disconnect
handling, reconnection, and name registration. It requires no change to the
base monad.

### 7.2 PULSE-style white-box scheduler over `IO`

Follow the `deterministic-scheduler` design: a `Signal` carried in the
`Process` reader environment, with `pause` inserted at the primitives that
matter, and `SingleThreaded` making it free in production.

`Internal/Primitives.hs` is the single choke point for all of this. There are
roughly twenty functions to instrument (`send`, `usend`, `receiveWait`,
`receiveTimeout`, `expect`, `spawn`, `link`, `monitor`, `exit`, `kill`, and
their variants), and they already all go through `sendCtrlMsg` or `CQueue`.
That is a cheap route to genuine interleaving control over user processes
without abstracting the monad.

Cost: random schedule exploration rather than partial order reduction, so
coverage is statistical, and shrinking fights the scheduler.

### 7.3 Abstract the base monad

The real thing. `Process` becomes `ProcessT m`, `Transport` becomes a record of
`m` actions, and the stack is `MonadAsync m => ...`. `IOSimPOR` then gives
virtual time, full traces, dynamic race discovery, and POR-bounded exploration,
which is the only option here that says anything rigorous about coverage.

The cost, measured against the current tree:

| Package | Modules touching concurrency |
| --- | --- |
| `distributed-process` | 13 of 27 |
| `distributed-process-extras` | 7 of 16 |
| `distributed-process-client-server` | 6 of 11 |
| `distributed-process-execution` | 3 of 7 |
| `distributed-process-async` | 2 of 2 |
| `network-transport` | 2 of 3 |
| `network-transport-inmemory` | 2 of 3 |
| `network-transport-tcp` | 3 of 4 |

On top of that it is an API break for every downstream user, `tested-with`
spans GHC 8.10.7 to 9.14.1 while `io-classes` requires `base >= 4.16`, and
changing `Transport` from a record of `IO` actions to a record of `m` actions
breaks `network-transport-tcp`, `network-transport-quic`, and every third-party
transport.

Recommendation: do not attempt this as a refactor of the existing packages. If
we want `io-sim`, the viable shape is a separate monad-polymorphic
`distributed-process-core`, with `distributed-process` as a thin
`type Process = ProcessT IO` shim, which is the `ouroboros-network` shape. That
is a multi-release project and should be decided on its own merits rather than
as a side effect of a testing effort. Put it behind Layers 0 to 2 and revisit
with real data about which bugs the cheaper layers failed to find.

## 8. Layer 4: model-based testing

The model already exists in prose. The implementation is written against the
Unified semantics, and the code comments cite it directly, for example
"[Unified table 9, rule node_disconnect]" in `handleIncomingMessages`. That
state-transition specification is the model. Writing it down as a `StateModel`
would be a valuable documentation artefact independent of any test it enables.

Recommendation:

- Start with `quickcheck-dynamic`, optionally with `quickcheck-lockstep` for
  the response comparison, for sequential model-based tests. The model is an
  idealised multi-node Cloud Haskell: nodes, processes, links, monitors,
  registrations, and mailboxes. Actions are `Spawn`, `Send`, `Link`, `Monitor`,
  `Exit`, `Kill`, `Register`, `WhereIs`, `Partition`, `Heal`, and
  `StepDelivery`. The partition and delivery actions are what make it
  interesting, and they come straight from Layer 1's `SimControl`.
- Reach for `quickcheck-dynamic`'s dynamic logic for the properties the current
  suite expresses with sleeps, such as "after this partition, every surviving
  monitor eventually fires exactly once".
- `quickcheck-state-machine` is the alternative if we want its built-in
  parallel command generation and linearisability checking. With Layers 1 and 2
  in place we do not need that, because we want the rig to own the
  interleaving, not the library. So `quickcheck-dynamic` is the better fit.
- Do not model mailbox ordering too strongly at first. Cloud Haskell's
  guarantees are weaker than people assume, ordered per sender-receiver pair,
  and `usend` is not even that. An over-strong model produces false failures
  and burns credibility. Model eventual delivery and set membership first, then
  tighten.

## 9. Sequencing

1. Layer 0: quiescence helpers, thread labels, and a `-C0` CI matrix. Kills the
   current flakiness and is independent of everything else.
2. Layer 1: `network-transport-sim` with `SimControl`. The concrete deliverable
   #419 asked for.
3. Layer 2: `CtrlQueue` and `NodeHooks`. Small and surgical, and unlocks the
   signal-ordering races.
4. Layer 3.1: quiescence stepping over Layers 1 and 2. Deterministic
   protocol-level tests.
5. Layer 4: the `quickcheck-dynamic` model. Where the real bug-finding starts.
6. Only then decide about 7.2 or 7.3.

## 10. References

- [GHC proposal #313, delimited continuation primops](https://ghc-proposals.readthedocs.io/en/latest/proposals/0313-delimited-continuation-primops.html)
- [Composable Scheduler Activations for Haskell](https://simonmar.github.io/bib/papers/composable_scheduler_activations.pdf)
- [GHC #7606, stride scheduling](https://gitlab.haskell.org/ghc/ghc/-/issues/7606)
- [GHC thread introspection merge request](https://gitlab.haskell.org/ghc/ghc/-/merge_requests/2816)
- [`GHC.Conc` (base-4.18)](https://hackage.haskell.org/package/base-4.18.0.0/docs/GHC-Conc.html)
- [`dejafu` on Hackage](https://hackage.haskell.org/package/dejafu)
- [Déjà Fu paper](https://eprints.whiterose.ac.uk/118198/1/DejaFu.pdf)
- [`io-sim`](https://github.com/IntersectMBO/io-sim) and [`io-classes`](https://hackage.haskell.org/package/io-classes)
- [IOSim on Hackage (IOG announcement)](https://engineering.iog.io/2023-04-14-io-sim-annoucement/)
- [An introduction to io-sim (Well-Typed, Oct 2025)](https://www.well-typed.com/blog/2025/10/an-introduction-to-io-sim/)
- [stevana/deterministic-scheduler](https://github.com/stevana/deterministic-scheduler)
- [Parallel property-based testing with a deterministic thread scheduler (Discourse)](https://discourse.haskell.org/t/parallel-property-based-testing-with-a-deterministic-thread-scheduler/10126)
- [Lockstep-style testing with quickcheck-dynamic (Well-Typed)](https://well-typed.com/blog/2022/09/lockstep-with-quickcheck-dynamic/)
- [`quickcheck-dynamic`](https://github.com/input-output-hk/quickcheck-dynamic), [`quickcheck-lockstep`](https://hackage.haskell.org/package/quickcheck-lockstep), [`quickcheck-state-machine`](https://hackage.haskell.org/package/quickcheck-state-machine)
