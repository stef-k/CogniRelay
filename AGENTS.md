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
- Use `Refs #<issue>` for tracking issues unless the PR truly completes the issue.
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
