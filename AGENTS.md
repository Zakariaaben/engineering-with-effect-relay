# Relay implementation contract

Relay is one cumulative teaching system. Implement only the change required by
the assigned course chapter; do not pull later architecture into an earlier
checkpoint.

When this repository is mounted under the course repository, also follow the
root course `AGENTS.md` and `course/RELAY_PROJECT_SPEC.md`.

## Workflow

- Require a concrete chapter ID before changing application code.
- Start from the previous accepted checkpoint on `main`.
- Use the same `chapter/<lowercase-chapter-id>-<short-slug>` as the course
  repository. One chapter gets one branch; individual examples and authoring
  steps do not get separate branches.
- Add a deterministic reproduction before the repair when practical.
- Keep commits coherent and the branch buildable.
- Run the focused chapter tests and the complete suite before handoff.
- Do not merge, push, or create checkpoint tags without explicit user
  authorization.

## Toolchain

- Bun `1.3.14` for installs, scripts, tests, and TypeScript execution.
- Effect `4.0.0-beta.98` and matching official Effect packages.
- TypeScript `7.0.2`.
- Node.js 24 as the application compatibility target unless a Bun-platform
  chapter explicitly changes the target.

Use `bun install --frozen-lockfile`, `bun add --exact`, `bun run`, `bun test`,
and `bunx`. Do not introduce another JavaScript package manager casually.

## Engineering rules

- Preserve every guarantee established by earlier checkpoints.
- Before building a utility or abstraction, search the pinned Effect public
  modules, official packages, tests, and examples for an existing semantic fit.
- Prefer the matching Effect primitive; otherwise use a clear native boundary
  or pure function, and build custom infrastructure only as a last resort.
- Do not recreate queues, schedules, scopes, retries, fiber registries, caches,
  batching, or resource management when Effect already provides the required
  semantics.
- Keep pure domain decisions independent of Effect.
- Decode unknown values at trust boundaries.
- Model expected failures explicitly and keep defects/interruption distinct.
- Own resources and fibers through Scope and structured concurrency.
- Bound queues, concurrency, retry, and memory-sensitive flow.
- Do not add tests for trivial constructors or transformations merely to raise
  coverage. Test non-trivial runtime properties and failure boundaries.
- Use deterministic synchronization and virtual time for time, retry,
  concurrency, interruption, and lifecycle tests; never arbitrary sleeps.
- Keep unsafe teaching fixtures isolated from the passing application.
- Never claim exactly-once remote side effects.
- Document which authority provides each guarantee and where it ends.
