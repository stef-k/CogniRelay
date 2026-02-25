#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  sudo bootstrap-debian-nginx.sh --domain <fqdn> [--email <letsencrypt-email>] [--src <repo-path>]

Options:
  --domain   Public DNS name for CogniRelay (required)
  --email    Email for certbot (optional; if omitted TLS is not provisioned)
  --src      Source path of local CogniRelay checkout (default: script parent repo)
USAGE
}

if [[ ${EUID} -ne 0 ]]; then
  echo "This script must run as root." >&2
  exit 1
fi

DOMAIN=""
EMAIL=""
SRC=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --domain)
      DOMAIN="${2:-}"
      shift 2
      ;;
    --email)
      EMAIL="${2:-}"
      shift 2
      ;;
    --src)
      SRC="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DOMAIN" ]]; then
  echo "--domain is required." >&2
  usage
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_SRC="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SRC="${SRC:-$DEFAULT_SRC}"

if [[ ! -d "$SRC/app" || ! -f "$SRC/requirements.txt" ]]; then
  echo "Invalid --src path: $SRC" >&2
  echo "Expected CogniRelay repository root containing app/ and requirements.txt" >&2
  exit 1
fi

APP_DIR="/opt/cognirelay"
SERVICE_USER="cognirelay"
SERVICE_GROUP="cognirelay"
DATA_DIR="/var/lib/cognirelay/repo"
SECURITY_DIR="/var/lib/cognirelay/security"
ETC_DIR="/etc/cognirelay"
ENV_FILE="${ETC_DIR}/cognirelay.env"
PEER_TOKENS_FILE="${DATA_DIR}/config/peer_tokens.json"
OPS_TOKEN_FILE="${ETC_DIR}/ops.token"
HOST_ADMIN_TOKEN_FILE="${ETC_DIR}/host_admin.token"

set_env_value() {
  local file="$1"
  local key="$2"
  local value="$3"
  if grep -qE "^${key}=" "$file"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

generate_or_read_token() {
  local file="$1"
  if [[ -s "$file" ]]; then
    tr -d '[:space:]' < "$file"
    return
  fi
  local token
  token="$(openssl rand -hex 32)"
  printf '%s\n' "$token" > "$file"
  chmod 0640 "$file"
  chown root:${SERVICE_GROUP} "$file"
  printf '%s' "$token"
}

echo "[1/9] Installing packages"
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3 python3-venv python3-pip \
  curl jq git rsync openssl ufw nginx \
  certbot python3-certbot-nginx

echo "[2/9] Creating user and directories"
useradd --system --home /var/lib/cognirelay --create-home --shell /usr/sbin/nologin "$SERVICE_USER" 2>/dev/null || true
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$APP_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$DATA_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$SECURITY_DIR"
install -d -o root -g "$SERVICE_GROUP" -m 0750 "$ETC_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "${DATA_DIR}/config"

echo "[3/9] Syncing application code"
rsync -a --delete \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude '.venv_local' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude 'data_repo' \
  "$SRC/" "$APP_DIR/"
chown -R "$SERVICE_USER:$SERVICE_GROUP" "$APP_DIR"

echo "[4/9] Creating virtualenv and installing requirements"
sudo -u "$SERVICE_USER" python3 -m venv "$APP_DIR/.venv"
sudo -u "$SERVICE_USER" "$APP_DIR/.venv/bin/pip" install --upgrade pip
sudo -u "$SERVICE_USER" "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "[5/9] Configuring environment"
if [[ ! -f "$ENV_FILE" ]]; then
  cp "$APP_DIR/deploy/systemd/cognirelay.env.example" "$ENV_FILE"
fi
chown root:"$SERVICE_GROUP" "$ENV_FILE"
chmod 0640 "$ENV_FILE"
set_env_value "$ENV_FILE" "COGNIRELAY_REPO_ROOT" "$DATA_DIR"
set_env_value "$ENV_FILE" "COGNIRELAY_REQUIRE_SIGNED_INGRESS" "true"
set_env_value "$ENV_FILE" "COGNIRELAY_USE_EXTERNAL_KEY_STORE" "true"
set_env_value "$ENV_FILE" "COGNIRELAY_KEY_STORE_PATH" "${SECURITY_DIR}/security_keys.json"
set_env_value "$ENV_FILE" "COGNIRELAY_TOKENS" ""

echo "[6/9] Installing systemd service and ops timers"
cp "$APP_DIR/deploy/systemd/cognirelay.service" /etc/systemd/system/
cp "$APP_DIR/deploy/systemd/"cognirelay-ops-*.service /etc/systemd/system/
cp "$APP_DIR/deploy/systemd/"cognirelay-ops-*.timer /etc/systemd/system/
install -o root -g root -m 0755 "$APP_DIR/deploy/scripts/cognirelay-ops-run.sh" /usr/local/bin/cognirelay-ops-run.sh
systemctl daemon-reload
systemctl enable --now cognirelay.service
systemctl enable --now \
  cognirelay-ops-index.timer \
  cognirelay-ops-metrics.timer \
  cognirelay-ops-backup.timer \
  cognirelay-ops-restore-test.timer \
  cognirelay-ops-rotation-check.timer \
  cognirelay-ops-compact-plan.timer

echo "[7/9] Generating local host tokens"
HOST_ADMIN_TOKEN="$(generate_or_read_token "$HOST_ADMIN_TOKEN_FILE")"
OPS_TOKEN="$(generate_or_read_token "$OPS_TOKEN_FILE")"
HOST_ADMIN_SHA="$(python3 "$APP_DIR/tools_hash_token.py" "$HOST_ADMIN_TOKEN")"
OPS_SHA="$(python3 "$APP_DIR/tools_hash_token.py" "$OPS_TOKEN")"

if [[ -f "$PEER_TOKENS_FILE" ]]; then
  echo "Existing $PEER_TOKENS_FILE detected; leaving it unchanged."
  echo "Ensure entries for host-admin and host-ops exist and use current token hashes."
else
  cat > "$PEER_TOKENS_FILE" <<JSON
{
  "tokens": [
    {
      "peer_id": "host-admin",
      "token_sha256": "${HOST_ADMIN_SHA}",
      "scopes": [
        "admin:peers",
        "read:files",
        "read:index",
        "write:journal",
        "write:messages",
        "write:projects",
        "search",
        "compact:trigger"
      ],
      "read_namespaces": ["*"],
      "write_namespaces": ["*"],
      "status": "active"
    },
    {
      "peer_id": "host-ops",
      "token_sha256": "${OPS_SHA}",
      "scopes": ["admin:peers", "read:index", "search", "compact:trigger"],
      "read_namespaces": ["*"],
      "write_namespaces": ["*"],
      "status": "active"
    }
  ]
}
JSON
  chown "$SERVICE_USER:$SERVICE_GROUP" "$PEER_TOKENS_FILE"
  chmod 0640 "$PEER_TOKENS_FILE"
fi

echo "[8/9] Configuring nginx"
cp "$APP_DIR/deploy/nginx/cognirelay.conf" /etc/nginx/sites-available/cognirelay.conf
sed -i "s|relay.example.com|${DOMAIN}|g" /etc/nginx/sites-available/cognirelay.conf
ln -sf /etc/nginx/sites-available/cognirelay.conf /etc/nginx/sites-enabled/cognirelay.conf
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl enable --now nginx
systemctl reload nginx

if [[ -n "$EMAIL" ]]; then
  echo "Requesting Let's Encrypt certificate for ${DOMAIN}"
  certbot --nginx --non-interactive --agree-tos --redirect -m "$EMAIL" -d "$DOMAIN"
fi

echo "[9/9] Applying firewall baseline"
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw deny 8080/tcp
ufw --force enable

echo
echo "Bootstrap complete."
echo "Domain: ${DOMAIN}"
if [[ -z "$EMAIL" ]]; then
  echo "TLS not provisioned (no --email provided). Run: certbot --nginx -d ${DOMAIN}"
fi
echo "Health check: curl -sS http://127.0.0.1:8080/health"
echo "Public check:  curl -sS https://${DOMAIN}/health"
