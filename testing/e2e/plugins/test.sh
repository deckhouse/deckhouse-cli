#!/bin/bash
# Prerequirements:
# - login to registry
# - d8 binary in ./bin/ (run task build)

# Inputs:
# ARG1 = registry login
# ARG2 = registry password

# setup
export DECKHOUSE_CLI_PATH="$(PWD)/e2e.test"
export DECKHOUSE_REGISTRY_REPO="dev-registry.deckhouse.io/deckhouse/foxtrot"
export D8_MIRROR_SOURCE_LOGIN=$1
export D8_MIRROR_SOURCE_PASSWORD=$2

# cleanup 
cleanup_on_error() {
    rm -rf ./e2e.test/
}
# Defer the removal of the temp file to the script's exit
trap cleanup_on_error EXIT ERR

# tests
echo "--- TEST LIST PLUGINS ---"
$(PWD)/bin/d8 plugins list # run task build before required 

echo ""
echo "--- TEST CLEAN INSTALL PLUGIN ---"
$(PWD)/bin/d8 plugins install package

echo ""
echo "--- TEST INSTALL SECOND PLUGIN ---"
$(PWD)/bin/d8 plugins install system
