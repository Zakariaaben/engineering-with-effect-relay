# Relay implementation contract

Relay is one cumulative teaching system. Implement only the change required by
the assigned course chapter; do not pull later architecture into an earlier
checkpoint.

When this repository is mounted under the course repository, also follow the
root course `AGENTS.md` and `course/RELAY_PROJECT_SPEC.md`.

## Workflow

- Require a concrete chapter ID before changing application code.
- Start from the previous accepted checkpoint on `main`.
- Use `chapter/<lowercase-chapter-id>-<short-slug>` for chapter work.
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
- Keep pure domain decisions independent of Effect.
- Decode unknown values at trust boundaries.
- Model expected failures explicitly and keep defects/interruption distinct.
- Own resources and fibers through Scope and structured concurrency.
- Bound queues, concurrency, retry, and memory-sensitive flow.
- Use deterministic synchronization and virtual time in tests; never arbitrary
  sleeps.
- Keep unsafe teaching fixtures isolated from the passing application.
- Never claim exactly-once remote side effects.
- Document which authority provides each guarantee and where it ends.
