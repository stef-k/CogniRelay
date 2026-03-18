# CogniRelay Repository Guidance

This file captures repo-specific workflow and hygiene rules for changes in this codebase.

## Branching and Git Safety

- Never do feature work directly on `main`.
- Start each non-trivial change on a dedicated branch.
- Create an early safety checkpoint commit before multi-file work.
- Keep branch history recoverable with small checkpoint commits during long refactors.
- Use `gh` for GitHub issue and PR operations.

## PR and Issue Hygiene

- Keep refactors and features in small, reviewable PRs.
- Use `Refs #<issue>` when the PR only tracks or partially addresses an issue.
- Use `Closes #<issue>` or `Fixes #<issue>` when merging the PR should automatically close the issue.
- Include the exact verification commands used in the PR body.
- Merge behavior-preserving refactor slices before starting the next slice.

## Testing Expectations

- Run targeted tests while implementing a slice.
- Before merge, run the full suite:
  - `./.venv/bin/python -m unittest discover -s tests -v`
- If a refactor changes module boundaries, update tests that patch internal implementation points so they target the extracted module instead of `app.main`.

## Architecture Boundaries

- Keep `app/main.py` focused on route registration, dependency wiring, and thin composition wrappers.
- Domain logic belongs in the extracted modules under `app/<domain>/service.py`.
- Preserve the public API surface unless the issue explicitly allows API changes.
- Prefer extracting existing logic over introducing new abstractions.

## Tooling

- The project currently uses `.venv` and `requirements.txt`.
- Run the repo-standard Ruff command before each commit and again before opening a PR.
- Do not mix broad tooling churn into feature or refactor PRs unless the issue is specifically about tooling.

## Documentation

- Add or update docstrings when touching public modules, classes, and functions.
- Keep docstrings concise and aligned with actual behavior.
- Do not add speculative comments about future behavior.

## Change Discipline

- Behavior-preserving refactors should not intentionally change endpoint semantics.
- Keep changes scoped to the active issue.
- Add targeted regression tests when moving risky logic.
- Favor clarity and explicit wiring over clever indirection.

## Continuity Resilience

- CogniRelay is a mission-critical system supporting autonomous agents running 24/7 as orientation and memory infrastructure.
- **No crashes or breakage under any circumstance.** The only acceptable outcome when an error occurs is graceful degraded performance and recovery. The system must never crash, return unhandled exceptions, or leave agents without a working path forward.
- Treat any change that could break continuity retrieval, corrupt stored state, or weaken durability guarantees as a production risk requiring extra verification.
- Treat continuity features as mission-critical agent-orientation infrastructure.
- Reads must never fail with an HTTP error. If a capsule is corrupted or missing, return a degraded response with warnings rather than a 4xx/5xx whenever the current API contract allows that behavior.
- Multi-step continuity mutations must preserve existing data on failure; never leave an agent without its last durable capsule because a later archive or commit step failed.
- When a continuity operation can degrade safely under the current contract, prefer returning the best available result over failing the whole aggregate operation.
- Skip unreadable or concurrently removed entries in list-style continuity views when doing so preserves a deterministic response.
- At no point should a refactor, feature addition, or tooling change knowingly leave the system in a state where a running agent could lose its continuity data without a recovery path.
- Surface continuity failures as warnings in the response body rather than HTTP status codes whenever the current endpoint contract permits that behavior; the agent decides how to react.
- Do not silently change established continuity API semantics in implementation code. If stronger resilience requires a contract change, record it in the appropriate roadmap/spec issue first.

## Review and Quality Standards

- Because CogniRelay is mission-critical, **all review findings must be reported and addressed regardless of severity** — from informational up to and including critical.
- No finding is too low-severity to skip. The cost of fixing a low-severity issue is far lower than the cost of it escalating in production against 24/7 agents.
- Code reviews, security audits, and automated checks must surface every finding. Each must be resolved before merging.
