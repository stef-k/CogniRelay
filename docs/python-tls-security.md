# Python TLS trust audit and recovery (#315)

Audit date: 2026-10-23. Scope: source deployment, Python wheel/sdist,
release builds, and the shipped HTTP clients. The
[Python packaging advisory](https://discuss.python.org/t/information-for-python-users-regarding-emudhra-emsign-ca-certificate-security-incident/109576)
recommends pip >=26.2.2 and certifi >=2026.10.19 for the eMudhra incident.
An old version identifies an affected trust bundle, not evidence that a
CogniRelay connection was intercepted.

## Trust boundaries

| Path | Trust source and action |
| --- | --- |
| Deployment's first Python download | `venv`/`ensurepip` can seed old pip. Install the advisory's exact pip 26.2.2 wheel with its SHA-256 before requirements. Disable index lookup, dependency resolution, cache reuse, and pip's version check for this step. |
| Subsequent dependency/build downloads | Patched pip uses its corrected vendored certifi plus the system store on supported Python. Keep both current; an old custom/system store can reintroduce trust. |
| Peer manifest and replication push | `app/peers/service.py` and `app/maintenance/service.py` use stdlib `urllib.request.urlopen`, with Python/OpenSSL's default system trust. No bundled application CA store. |
| CLI client and shipped agent hooks | `tools/cognirelay_client.py` and the retrieval/save hooks also use default `urllib` trust. HTTPS requires patched host certificates. Loopback HTTP does not use TLS. |
| MCP schema validator | `tools/validate_server_json.py` uses default `urllib` trust for its schema download. A local `--schema` file also works. |
| Development/release HTTP clients | HTTPX and Twine use standalone certifi by default. `requirements-dev.txt` now requires certifi >=2026.10.19; update existing release environments explicitly. |
| Distributed artifacts | The pure-Python wheel and sdist do not embed Python, pip, OpenSSL, certifi, or a CA bundle. Installation depends on the caller's environment; release notes cannot repair an existing venv. The sdist/source archive includes deployment instructions and scripts; wheel metadata includes the PyPI install guide. |

Runtime dependencies remain FastAPI, Uvicorn, Pydantic, python-dotenv,
Markdown, and Bleach. No runtime certifi dependency is added: application
HTTPS uses the system store. This patch changes the distributed bootstrap
and installation contract, so a patch artifact is appropriate.

## Install and recover existing environments

Apply your distribution's CA security update first. On Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install --only-upgrade ca-certificates
```

Use the distribution's security repositories. Ubuntu 24.04's corrected
package is `20260816ubuntu0.24.04.1` per
[USN-8944-1](https://ubuntu.com/security/notices/USN-8944-1);
Debian 13's is `20250419+deb13u1` per
[DSA-6572-1](https://lists.debian.org/debian-security-announce/2026/msg00487.html).
Other supported releases need their corresponding vendor update.
The bootstrap explicitly installs `ca-certificates` through apt before pip.

Create/activate a venv, or activate the existing venv to repair it. Run:

```bash
python -I -m pip --isolated --disable-pip-version-check install --no-index --no-deps --no-cache-dir --upgrade \
  "pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"
```

The hash comes from the Python advisory and
[PyPI's pip 26.2.2 metadata](https://pypi.org/pypi/pip/26.2.2/json).
Even if old TLS trust permits a substituted response, pip rejects bytes
that do not match the pinned digest before installation. TLS verification
stays enabled. If download, TLS, or hash validation fails, stop and repair
the network/trust configuration; do not bypass verification. This command
deliberately selects the advisory's fixed pip version, including when a
newer pip is already installed; subsequent upgrades must retain >=26.2.2.
It does not invoke `ensurepip --upgrade-deps`, which would download first.

Then install requirements, or upgrade the installed CogniRelay package.
If standalone certifi is present, update it through the corrected pip:

```bash
python -m pip install --upgrade 'certifi>=2026.10.19'
```

For development/release environments, install `requirements-dev.txt` after
the pinned bootstrap. Build isolation uses the invoking build tool's pip
to fetch build requirements; bootstrap that environment before `python -m
build` and run `python -m twine check` before publication. Fresh venvs need
the bootstrap each time until their seeded pip is corrected. Keep Ubuntu's
externally managed system Python intact: no system pip replacement,
`sudo pip`, or `--break-system-packages` is required.

Custom `SSL_CERT_FILE`, `SSL_CERT_DIR`, `PIP_CERT`, `REQUESTS_CA_BUNDLE`,
`CURL_CA_BUNDLE`, and pip configuration can alter trust. Audit their actual
CA contents separately; updating certifi alone does not sanitize overrides
or the system store. Do not add a removed root back to work around a failed
connection. Python.org macOS installs can point default SSL trust at a
separate certifi bundle; repair that interpreter's certificates as described
in the Python advisory. Windows/macOS deployments were not exercised here.

This maintenance does not require moving continuity state, changing tokens,
recreating the data repo, or rerunning host provisioning. Existing service
installation, rollback, durable storage, and API error contracts are unchanged.
Do not rerun the full provisioning script merely to repair an old pip.

## Observed environment and evidence limits

The audit host runs Ubuntu 24.04, Python 3.12.3, OpenSSL 3.0.13, and
`ca-certificates 20260816ubuntu0.24.04.1`. Default SSL paths resolve through
`/usr/lib/ssl/cert.pem` to `/etc/ssl/certs/ca-certificates.crt`.
The corrected system bundle contained neither `emSign ECC Root CA - G3`
nor `AAA Certificate Services` when inspected through `ssl`.

Before maintenance, this checkout's venv had Ubuntu-packaged pip 24.0
(`24.0+dfsg-1ubuntu1.3` seed) whose vendored certifi reported 2023.07.22 but
actually resolved to the corrected system bundle. Standalone certifi
2026.2.25, used by HTTPX 0.28.1 and Requests 2.33.1/Twine 6.2.0, still
included the affected G3 root. Therefore the old pip version alone did not
prove exposure on this patched Ubuntu host; the standalone store needed
maintenance. The pinned bootstrap installed pip 26.2.2 with vendored
certifi 2026.10.19, followed by standalone certifi 2026.10.19. Inspection
confirmed both corrected bundles omitted the affected root.

Build tooling observed: build 1.4.4. Runtime packages observed: FastAPI
0.135.1, Uvicorn 0.41.0, Pydantic 2.12.5, python-dotenv 1.2.2, Markdown
3.10.2, and Bleach 6.3.0. These are an audit snapshot, not new version pins.
Regression tests exercise the deployment command's first-download contract
and a real pip hash rejection, including stopping before the next install.
Full nginx/systemd provisioning and remote production hosts are outside
this local verification; their installed CA contents must be checked by
their operators. No exploitation or production compromise was demonstrated.
