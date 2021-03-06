#!/usr/bin/env bash
set -e

# Meant to run only in CI
if [ -z "${DRONE}" ]; then
  echo "Must be run on Drone!"
  exit 1
fi

DRONE_ROOT_DIR="/drone/src"
SCENARIOS_DIR="$DRONE_ROOT_DIR/utils/nctl/sh/scenarios"
LAUNCHER_DIR="/drone"

# NCTL requires casper-node-launcher
pushd $LAUNCHER_DIR
git clone https://github.com/CasperLabs/casper-node-launcher.git

# Activate Environment
pushd $DRONE_ROOT_DIR
source $(pwd)/utils/nctl/activate
# Build, Setup, and Start NCTL
nctl-compile
nctl-assets-setup
nctl-start
echo "Sleeping 60 to allow network startup"
sleep 60

# Switch to scenarios
pushd $SCENARIOS_DIR
source sync_test.sh node=6 timeout=500

# Switch back and teardown
popd
nctl-assets-teardown

# Clean up cloned repo
popd
echo "Removing $LAUNCHER_DIR/casper-node-launcher"
rm -rf "$LAUNCHER_DIR/casper-node-launcher"
