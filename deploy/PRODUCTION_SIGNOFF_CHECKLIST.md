# CogniRelay Production Sign-Off Checklist (Debian/Ubuntu + nginx)

Operator: ____________________
Date: ____________________
Environment: ____________________
Domain: ____________________

## Deployment baseline

- [ ] Host OS is Debian/Ubuntu with latest security updates applied.
- [ ] DNS A/AAAA records point to intended host.
- [ ] Deployment performed using `deploy/GO_LIVE_RUNBOOK.md` or `deploy/scripts/bootstrap-debian-nginx.sh`.
- [ ] CogniRelay service is enabled and running: `systemctl status cognirelay.service`.
- [ ] App responds locally: `curl http://127.0.0.1:8080/health`.

## Reverse proxy and TLS

- [ ] nginx config is active and valid (`nginx -t` passes).
- [ ] TLS certificate is issued and valid (`certbot certificates` or equivalent).
- [ ] HTTPS endpoint responds: `curl https://<domain>/health`.
- [ ] HTTP is redirected to HTTPS.
- [ ] External access to `/v1/ops/*` is blocked by nginx (returns 403).

## Security model

- [ ] `COGNIRELAY_REQUIRE_SIGNED_INGRESS=true` is set in `/etc/cognirelay/cognirelay.env`.
- [ ] `COGNIRELAY_USE_EXTERNAL_KEY_STORE=true` is set.
- [ ] Key store path points outside source tree (for example `/var/lib/cognirelay/security/security_keys.json`).
- [ ] `COGNIRELAY_TOKENS` env value is empty in production.
- [ ] File-based tokens exist in `repo/config/peer_tokens.json` using `token_sha256`.
- [ ] Host-only secrets (`/etc/cognirelay/ops.token`, `/etc/cognirelay/host_admin.token`) have restricted perms (`0640`, root:cognirelay).
- [ ] Firewall policy allows only required inbound ports (`22`, `80`, `443`) and blocks direct `8080`.

## Host ops automation

- [ ] Ops runner script is installed: `/usr/local/bin/cognirelay-ops-run.sh`.
- [ ] All expected timers are enabled (`cognirelay-ops-*.timer`).
- [ ] Local ops catalog succeeds with host ops token on loopback.
- [ ] Ops runs are being recorded in `logs/ops_runs.jsonl`.
- [ ] No recurring failed ops jobs in `journalctl` or ops logs.

## Data safety and recovery

- [ ] Backup creation works (`POST /v1/backup/create`).
- [ ] Restore drill works (`POST /v1/backup/restore-test`).
- [ ] Backup artifacts are stored on intended durable storage.
- [ ] Recovery procedure is documented and tested by operator.

## Federation and collaboration checks

- [ ] `/v1/discovery`, `/v1/manifest`, and MCP endpoint are reachable as expected.
- [ ] Token issuance/revocation workflow is validated with a non-production peer token.
- [ ] Trust transition workflow is validated for at least one test peer.
- [ ] Replication pull/push tested in staging or controlled peer environment.

## Observability and audit

- [ ] API audit log exists and is writable (`logs/api_audit.jsonl`).
- [ ] Metrics endpoint is accessible and reviewed (`/v1/metrics`).
- [ ] Alarm thresholds are explicitly set and reviewed for current workload.
- [ ] Monitoring/alerting hooks (if used) are validated.

## Release gate

- [ ] Automated test suite is green on release artifact.
- [ ] Config diff and secret changes are reviewed by operator.
- [ ] Rollback plan is available (`deploy/scripts/rollback-debian-nginx.sh`).
- [ ] Final go-live approval recorded.

Sign-off result:

- [ ] APPROVED FOR PUBLIC TRAFFIC
- [ ] NOT APPROVED

Notes:

______________________________________________________________________________
______________________________________________________________________________
______________________________________________________________________________

