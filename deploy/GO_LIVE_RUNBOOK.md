# CogniRelay Go-Live Runbook (Debian/Ubuntu + nginx)

This runbook targets a general Debian/Ubuntu host using `systemd`, `nginx`, and optional Let's Encrypt TLS via `certbot`.

## 1. Host assumptions

- Debian 12+ or Ubuntu 22.04/24.04
- Public DNS name already pointed to host IP (example: `relay.example.com`)
- Service user: `cognirelay`
- App install path: `/opt/cognirelay`
- Data repo path: `/var/lib/cognirelay/repo`

## 2. Automated bootstrap (recommended)

Run the bootstrap script from this repository as root:

```bash
sudo /path/to/CogniRelay/deploy/scripts/bootstrap-debian-nginx.sh \
  --domain relay.example.com \
  --email ops@example.com \
  --src /path/to/CogniRelay
```

What it does:

- installs required packages (`python3`, `venv`, `nginx`, `ufw`, `certbot`, etc.)
- creates user + directories
- installs app to `/opt/cognirelay`
- creates virtualenv and installs dependencies
- installs `systemd` units and ops timers
- generates host tokens and writes hashed entries to `peer_tokens.json`
- configures nginx reverse proxy and blocks external `/v1/ops/*`
- enables firewall baseline
- optionally provisions TLS cert via certbot (`--email`)

## 3. Manual install path (if not using bootstrap)

### 3.1 Base packages

```bash
sudo apt-get update
sudo apt-get install -y \
  python3 python3-venv python3-pip \
  curl jq git rsync openssl ufw nginx \
  certbot python3-certbot-nginx
```

### 3.2 Service user and directories

```bash
sudo useradd --system --home /var/lib/cognirelay --create-home --shell /usr/sbin/nologin cognirelay || true
sudo install -d -o cognirelay -g cognirelay -m 0750 /opt/cognirelay
sudo install -d -o cognirelay -g cognirelay -m 0750 /var/lib/cognirelay/repo
sudo install -d -o cognirelay -g cognirelay -m 0750 /var/lib/cognirelay/security
sudo install -d -o root -g cognirelay -m 0750 /etc/cognirelay
```

### 3.3 App install

```bash
sudo rsync -a --delete /path/to/CogniRelay/ /opt/cognirelay/
sudo chown -R cognirelay:cognirelay /opt/cognirelay

sudo -u cognirelay python3 -m venv /opt/cognirelay/.venv
sudo -u cognirelay /opt/cognirelay/.venv/bin/pip install --upgrade pip
sudo -u cognirelay /opt/cognirelay/.venv/bin/pip install -r /opt/cognirelay/requirements.txt
```

### 3.4 Environment file

```bash
sudo cp /opt/cognirelay/deploy/systemd/cognirelay.env.example /etc/cognirelay/cognirelay.env
sudo chown root:cognirelay /etc/cognirelay/cognirelay.env
sudo chmod 0640 /etc/cognirelay/cognirelay.env
```

Set at minimum:

- `COGNIRELAY_REPO_ROOT=/var/lib/cognirelay/repo`
- `COGNIRELAY_REQUIRE_SIGNED_INGRESS=true`
- `COGNIRELAY_KEY_STORE_PATH=/var/lib/cognirelay/security/security_keys.json`
- leave `COGNIRELAY_TOKENS=` empty when using file-backed tokens

### 3.5 Host token generation (hashed storage)

```bash
HOST_ADMIN_TOKEN="$(openssl rand -hex 32)"
HOST_OPS_TOKEN="$(openssl rand -hex 32)"

HOST_ADMIN_SHA="$(python3 /opt/cognirelay/tools/cognirelay_client.py token hash --value "$HOST_ADMIN_TOKEN")"
HOST_OPS_SHA="$(python3 /opt/cognirelay/tools/cognirelay_client.py token hash --value "$HOST_OPS_TOKEN")"

sudo install -d -o cognirelay -g cognirelay -m 0750 /var/lib/cognirelay/repo/config
cat <<JSON | sudo tee /var/lib/cognirelay/repo/config/peer_tokens.json >/dev/null
{
  "tokens": [
    {
      "peer_id": "host-admin",
      "token_sha256": "${HOST_ADMIN_SHA}",
      "scopes": [
        "admin:peers", "read:files", "read:index", "write:journal",
        "write:messages", "write:projects", "search", "compact:trigger"
      ],
      "read_namespaces": ["*"],
      "write_namespaces": ["*"],
      "status": "active"
    },
    {
      "peer_id": "host-ops",
      "token_sha256": "${HOST_OPS_SHA}",
      "scopes": ["admin:peers", "read:index", "search", "compact:trigger"],
      "read_namespaces": ["*"],
      "write_namespaces": ["*"],
      "status": "active"
    }
  ]
}
JSON

sudo chown cognirelay:cognirelay /var/lib/cognirelay/repo/config/peer_tokens.json
sudo chmod 0640 /var/lib/cognirelay/repo/config/peer_tokens.json

printf '%s\n' "$HOST_OPS_TOKEN" | sudo tee /etc/cognirelay/ops.token >/dev/null
printf '%s\n' "$HOST_ADMIN_TOKEN" | sudo tee /etc/cognirelay/host_admin.token >/dev/null
sudo chown root:cognirelay /etc/cognirelay/ops.token /etc/cognirelay/host_admin.token
sudo chmod 0640 /etc/cognirelay/ops.token /etc/cognirelay/host_admin.token
```

### 3.6 App service + ops timers

```bash
sudo cp /opt/cognirelay/deploy/systemd/cognirelay.service /etc/systemd/system/
sudo cp /opt/cognirelay/deploy/systemd/cognirelay-ops-*.service /etc/systemd/system/
sudo cp /opt/cognirelay/deploy/systemd/cognirelay-ops-*.timer /etc/systemd/system/
sudo install -o root -g root -m 0755 /opt/cognirelay/deploy/scripts/cognirelay-ops-run.sh /usr/local/bin/cognirelay-ops-run.sh

sudo systemctl daemon-reload
sudo systemctl enable --now cognirelay.service
sudo systemctl enable --now \
  cognirelay-ops-index.timer \
  cognirelay-ops-metrics.timer \
  cognirelay-ops-backup.timer \
  cognirelay-ops-restore-test.timer \
  cognirelay-ops-rotation-check.timer \
  cognirelay-ops-compact-plan.timer
```

> **Single-worker requirement:** The service unit runs one uvicorn process (no `--workers` flag). Do not add multiple workers without first migrating the rate-limit lock to cross-process file locking. See [Runtime Concurrency Model](../docs/system-overview.md#runtime-concurrency-model).

### 3.7 nginx reverse proxy + TLS

```bash
sudo cp /opt/cognirelay/deploy/nginx/cognirelay.conf /etc/nginx/sites-available/cognirelay.conf
sudo sed -i 's/relay.example.com/your.domain.example/g' /etc/nginx/sites-available/cognirelay.conf
sudo ln -sf /etc/nginx/sites-available/cognirelay.conf /etc/nginx/sites-enabled/cognirelay.conf
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx

# TLS certificate
sudo certbot --nginx -d your.domain.example
```

### 3.8 Firewall baseline

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw deny 8080/tcp
sudo ufw --force enable
sudo ufw status verbose
```

## 4. Final go-live validation

```bash
# local backend health
curl -sS http://127.0.0.1:8080/health | jq .ok

# public TLS endpoint
curl -sS https://relay.example.com/health | jq .ok
curl -sS https://relay.example.com/v1/discovery | jq .ok

# local-only ops boundary: should be forbidden externally
curl -i -sS https://relay.example.com/v1/ops/catalog

# local host ops should work
OPS_TOKEN="$(sudo cat /etc/cognirelay/ops.token)"
curl -sS http://127.0.0.1:8080/v1/ops/catalog -H "Authorization: Bearer ${OPS_TOKEN}" | jq .ok

# logs
sudo -u cognirelay test -f /var/lib/cognirelay/repo/logs/api_audit.jsonl
sudo -u cognirelay test -f /var/lib/cognirelay/repo/logs/ops_runs.jsonl
```

## 5. Rollback

If deployment must be reverted, use:

```bash
sudo /opt/cognirelay/deploy/scripts/rollback-debian-nginx.sh --domain relay.example.com
```

Optional destructive cleanup is explicit (`--purge-app`, `--purge-data`, `--purge-etc`, `--purge-user`).

## 6. Operations cadence

- Daily: review `/v1/metrics`, failed ops runs, disk usage.
- Weekly: verify restore drill output and replication drift.
- Monthly: rotate host tokens and signing keys.
- Per release: run test suite before deploy and smoke checks after deploy.

