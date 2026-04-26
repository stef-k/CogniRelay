# CogniRelay Agent Assets

This directory contains copyable integration assets for agent runtimes that use CogniRelay continuity.

- `skills/cognirelay-continuity-authoring/SKILL.md`: agent-facing continuity authoring guidance.
- `hooks/cognirelay_retrieval_hook.py`: read-only startup and optional context retrieval hook.
- `hooks/cognirelay_continuity_save_hook.py`: facts, template, dry-run, write, readback, and doctor hook.
- `hooks/README.md`: hook configuration, invocation, stdout, and exit-code contract.

CogniRelay is the continuity substrate. The running agent authors semantic continuity fields; hooks gather facts, provide scaffolds, validate, write explicit payloads, and read back stored state.
