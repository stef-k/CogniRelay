# Publishing a release

Prepare and review the patch, synchronize versions with
`tools/prepare_release.py`, and merge through a PR. The manual
`.github/workflows/publish.yml` workflow publishes an existing release tag
only when dispatched from `main`, and verifies that the tag is merged into
`main`. It runs the release checks, Ruff, the full suite, artifact builds,
and Twine metadata checks before the upload job.

## Trusted publisher setup

In the existing `cognirelay` PyPI project's
[publishing settings](https://pypi.org/manage/project/cognirelay/settings/publishing/),
add a GitHub Actions trusted publisher with these exact fields:

| Field | Value |
| --- | --- |
| Owner | `stef-k` |
| Repository | `CogniRelay` |
| Workflow filename | `publish.yml` |
| Environment | `pypi` |

The corresponding GitHub environment is `pypi`. Restrict its deployment
branches to `main`. No PyPI API token or repository secret is needed.
The upload job alone has `id-token: write`; it does not check out or execute
project source. Twine >=6.2 performs the OIDC exchange natively, using
certifi >=2026.10.19. Both jobs authenticate their first pip wheel download
before installing tooling. The publisher setup itself requires PyPI project
owner access; a GitHub credential cannot register it on PyPI.

## Publish and verify

After independent review, green validation, and merge, create and push the
release tag at the reviewed merge commit. Then dispatch from `main`:

```bash
gh workflow run publish.yml --ref main -f tag=v1.4.15
```

Watch that exact run with `gh run view`/`gh run watch`. After PyPI confirms
both files, download the run's `distributions` artifact using `gh run
download` and attach those same wheel/sdist bytes to the GitHub Release.
Publish/submit the updated `server.json` to the MCP Registry where configured.
Verify the published versions and file digests. Publication is manual;
merging a PR, pushing a tag, or publishing a GitHub Release does not by
itself trigger this workflow. An existing PyPI version is an error rather
than a silently skipped upload.
