# gsb-quota scheduler

Multi-tenant task scheduler with per-tenant token-bucket quotas and
weighted-fair dispatching. JDK 8, standard library only, no build tool.

## Layout

- `src/com/gsb/quota/` — library (`Scheduler`, `Clock`, `SystemClock`, `ManualClock`)
- `src/com/gsb/quota/tests/` — plain `main()`-based tests, no JUnit
- `run-tests.sh` — compiles everything with `javac` and runs the suite

## API

```java
Scheduler scheduler = new Scheduler();                 // or new Scheduler(clock, workerThreads)
scheduler.register("tenant", 1000L, 3);                // tokensPerSecond, weight
String taskId = scheduler.submit("tenant", callable);  // never drops; queues past quota
scheduler.start();
scheduler.shutdown();
```

## Design

- Quota: per-tenant token bucket, capacity = `tokensPerSecond`, refilled once
  per one-second period of `Clock` time. Releases inside any aligned
  one-second window never exceed the quota; over-quota submissions wait in
  the tenant's queue.
- Fairness: a single dispatcher picks the eligible tenant with the smallest
  virtual finish time and charges `1/weight` per release, so long-run
  completion counts converge to the weight ratio and a backlogged tenant is
  never starved for more than one second of clock time.
- Time: all scheduling reads come from the injected `Clock`; tests drive a
  `ManualClock`, so the suite finishes in real milliseconds.
- Submission is not serialized through a global lock: `submit()` only
  touches the target tenant's concurrent queue and signals the dispatcher.

## Run

```bash
./run-tests.sh
```
