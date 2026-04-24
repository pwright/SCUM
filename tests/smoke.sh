#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TMPDIR_PATH="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_PATH"' EXIT

cp sysmvp.py schema.sql .sysignore "$TMPDIR_PATH"/
cd "$TMPDIR_PATH"

python3 sysmvp.py init >/tmp/sysmvp-init.out 2>/tmp/sysmvp-init.err
mkdir -p demo
printf 'alpha\n' > demo/a.txt
printf 'beta\n' > demo/b.txt
printf 'solo\n' > single.txt
python3 sysmvp.py scan --root demo >/tmp/sysmvp-scan.out 2>/tmp/sysmvp-scan.err
python3 sysmvp.py scan --file single.txt >/tmp/sysmvp-scan-file.out 2>/tmp/sysmvp-scan-file.err
python3 sysmvp.py list > actual.txt 2>/tmp/sysmvp-list.err

cat > expected.txt <<'EOT'
1	demo/a.txt	text/plain	text
2	demo/b.txt	text/plain	text
3	single.txt	text/plain	text
EOT

diff -u expected.txt actual.txt
printf '[smoke] PASS\n' >&2

cd "$ROOT_DIR"
python3 tests/image_metadata_extension_smoke.py >/tmp/image-extension-smoke.out 2>/tmp/image-extension-smoke.err
cat /tmp/image-extension-smoke.out >&2
python3 tests/pdf_preview_extension_smoke.py >/tmp/pdf-preview-extension-smoke.out 2>/tmp/pdf-preview-extension-smoke.err
cat /tmp/pdf-preview-extension-smoke.out >&2
python3 tests/root_watch_smoke.py >/tmp/root-watch-smoke.out 2>/tmp/root-watch-smoke.err
cat /tmp/root-watch-smoke.out >&2
python3 tests/server_smoke.py >/tmp/sysbrowse-smoke.out 2>/tmp/sysbrowse-smoke.err
cat /tmp/sysbrowse-smoke.out >&2
