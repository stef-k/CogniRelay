#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage:
  sudo rollback-debian-nginx.sh [options]

Options:
  --domain <fqdn>           Domain used for certbot cleanup (required with --remove-certificate)
  --firewall                Remove UFW rules added by bootstrap (80/443 allow, 8080 deny)
  --remove-certificate      Delete Let's Encrypt certificate for --domain
  --purge-app               Remove /opt/cognirelay
  --purge-data              Remove /var/lib/cognirelay/repo and /var/lib/cognirelay/security
  --purge-etc               Remove /etc/cognirelay
  --purge-user              Remove system user/group 'cognirelay' if possible
  --yes                     Skip confirmation prompt
  -h, --help                Show this help

Default behavior is non-destructive for data and secrets.
USAGE
}

if [[ ${EUID} -ne 0 ]]; then
  echo "This script must run as root." >&2
  exit 1
fi

DOMAIN=""
DO_FIREWALL="false"
DO_REMOVE_CERT="false"
DO_PURGE_APP="false"
DO_PURGE_DATA="false"
DO_PURGE_ETC="false"
DO_PURGE_USER="false"
ASSUME_YES="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --domain)
      DOMAIN="${2:-}"
      shift 2
      ;;
    --firewall)
      DO_FIREWALL="true"
      shift
      ;;
    --remove-certificate)
      DO_REMOVE_CERT="true"
      shift
      ;;
    --purge-app)
      DO_PURGE_APP="true"
      shift
      ;;
    --purge-data)
      DO_PURGE_DATA="true"
      shift
      ;;
    --purge-etc)
      DO_PURGE_ETC="true"
      shift
      ;;
    --purge-user)
      DO_PURGE_USER="true"
      shift
      ;;
    --yes)
      ASSUME_YES="true"
      shift
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

if [[ "$DO_REMOVE_CERT" == "true" && -z "$DOMAIN" ]]; then
  echo "--remove-certificate requires --domain" >&2
  exit 1
fi

if [[ "$ASSUME_YES" != "true" ]]; then
  echo "This will disable CogniRelay services and remove nginx site wiring."
  echo "Optional purge flags will delete files/users if selected."
  read -r -p "Continue? [y/N] " ans
  if [[ "${ans}" != "y" && "${ans}" != "Y" ]]; then
    echo "Aborted."
    exit 0
  fi
fi

SERVICE_UNITS=(
  "cognirelay.service"
  "cognirelay-ops-index.timer"
  "cognirelay-ops-index.service"
  "cognirelay-ops-metrics.timer"
  "cognirelay-ops-metrics.service"
  "cognirelay-ops-backup.timer"
  "cognirelay-ops-backup.service"
  "cognirelay-ops-restore-test.timer"
  "cognirelay-ops-restore-test.service"
  "cognirelay-ops-rotation-check.timer"
  "cognirelay-ops-rotation-check.service"
  "cognirelay-ops-compact-plan.timer"
  "cognirelay-ops-compact-plan.service"
)

echo "[1/6] Disabling and stopping systemd units"
for unit in "${SERVICE_UNITS[@]}"; do
  systemctl disable --now "$unit" >/dev/null 2>&1 || true
done

echo "[2/6] Removing systemd unit files"
rm -f /etc/systemd/system/cognirelay.service
rm -f /etc/systemd/system/cognirelay-ops-*.service
rm -f /etc/systemd/system/cognirelay-ops-*.timer
rm -f /usr/local/bin/cognirelay-ops-run.sh
systemctl daemon-reload

echo "[3/6] Removing nginx site wiring"
rm -f /etc/nginx/sites-enabled/cognirelay.conf
rm -f /etc/nginx/sites-available/cognirelay.conf
if command -v nginx >/dev/null 2>&1; then
  nginx -t >/dev/null 2>&1 || true
  systemctl reload nginx >/dev/null 2>&1 || true
fi

if [[ "$DO_REMOVE_CERT" == "true" ]]; then
  echo "[4/6] Removing certificate for ${DOMAIN}"
  if command -v certbot >/dev/null 2>&1; then
    certbot delete --cert-name "$DOMAIN" --non-interactive >/dev/null 2>&1 || true
  else
    echo "certbot not installed; skipping certificate cleanup"
  fi
else
  echo "[4/6] Certificate cleanup skipped"
fi

if [[ "$DO_FIREWALL" == "true" ]]; then
  echo "[5/6] Rolling back firewall rules added by bootstrap"
  yes | ufw delete allow 80/tcp >/dev/null 2>&1 || true
  yes | ufw delete allow 443/tcp >/dev/null 2>&1 || true
  yes | ufw delete deny 8080/tcp >/dev/null 2>&1 || true
else
  echo "[5/6] Firewall rollback skipped"
fi

echo "[6/6] Optional purge actions"
if [[ "$DO_PURGE_APP" == "true" ]]; then
  rm -rf /opt/cognirelay
  echo "- Removed /opt/cognirelay"
fi
if [[ "$DO_PURGE_DATA" == "true" ]]; then
  rm -rf /var/lib/cognirelay/repo /var/lib/cognirelay/security
  echo "- Removed /var/lib/cognirelay/repo and /var/lib/cognirelay/security"
fi
if [[ "$DO_PURGE_ETC" == "true" ]]; then
  rm -rf /etc/cognirelay
  echo "- Removed /etc/cognirelay"
fi
if [[ "$DO_PURGE_USER" == "true" ]]; then
  userdel --remove cognirelay >/dev/null 2>&1 || true
  groupdel cognirelay >/dev/null 2>&1 || true
  echo "- Attempted removal of user/group cognirelay"
fi

echo "Rollback completed."
