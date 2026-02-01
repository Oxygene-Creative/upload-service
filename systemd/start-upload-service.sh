#!/usr/bin/env bash
set -euo pipefail

cd "/home/peter.njuguna@ad.oxygenehq.com/dev/monitor-workspace/upload-service"

/usr/bin/docker compose up -d --remove-orphans
