#!/bin/bash
# Installs qq on your current desktop and on the computing nodes of all Metacentrum clusters.
# Script version: 0.7.0

set -euo pipefail

# -----------------------
# Configuration
# -----------------------

# qq version to install
QQ_VERSION="v__VERSION__"

# GitHub release assets
INSTALL_SCRIPT_URL="https://github.com/VachaLab/qq/releases/download/${QQ_VERSION}/qq-install.sh"
SETUP_SCRIPT_URL="https://github.com/VachaLab/qq/releases/download/${QQ_VERSION}/qq-set-scratch.sh"
RELEASE_URL="https://github.com/VachaLab/qq/releases/download/${QQ_VERSION}/qq-release.tar.gz"

# main home directory where source qq will be stored for the entire metacentrum
MAIN_HOME="/storage/brno12-cerit/home/${USER}"

# list of home directories of all nodes
TARGET_HOMES=(
    # charon
    "/storage/liberec3-tul/home/${USER}"
    # elmo
    "/storage/praha5-elixir/home/${USER}"
    # nympha
    "/storage/plzen1/home/${USER}"
    # oven, perian, onyx, skirit
    "/storage/brno2/home/${USER}"
    # tarkil
    "/storage/praha1/home/${USER}"
    # tilia
    "/storage/pruhonice1-ibot/home/${USER}"
    # zenith
    "/storage/brno12-cerit/home/${USER}"
    # computing nodes (galdor, halmir, tyra, aman)
    "/storage/brno2/home/${USER}"
    # computing nodes (pcr, fau, fer, mor)
    "/storage/praha2-natur/home/${USER}"
    # computing nodes (hildor)
    "/storage/budejovice1/home/${USER}"
    # computing nodes (elmu)
    "/storage/brno11-elixir/home/${USER}"
    # other storages
    "/storage/brno3-cerit/home/${USER}"
    "/storage/vestec1-elixir/home/${USER}"
)

# computers with local home directories that require SSH installation
LOCAL_HOME_HOSTS=(
    "samson"
)
# -----------------------
# Helpers
# -----------------------

# array membership test that is safe with empty arrays under 'set -u'
contains() {
    local needle="$1"
    shift
    local item
    for item in "$@"; do
        [ "$item" = "$needle" ] && return 0
    done
    return 1
}

# -----------------------
# Main logic
# -----------------------

# only install qq to the main home directory
TMP_INSTALLER="$(mktemp)"

echo "INFO    [qq metacentrum installer] Downloading qq installer from ${INSTALL_SCRIPT_URL}..."
curl -fsSL -o "$TMP_INSTALLER" "$INSTALL_SCRIPT_URL"
chmod +x "$TMP_INSTALLER"

echo "INFO    [qq metacentrum installer] Installing qq ${QQ_VERSION} from ${RELEASE_URL} into ${MAIN_HOME}..."
if [ -d "$MAIN_HOME" ]; then
    "$TMP_INSTALLER" "$MAIN_HOME" "$RELEASE_URL"
else
    echo "ERROR   [qq metacentrum installer] Main home directory is not available."
    exit 1
fi

# add qq from the main home directory to PATH in .bashrc files in all home directories
TMP_SETUP="$(mktemp)"
echo "INFO    [qq metacentrum installer] Downloading qq setup from ${SETUP_SCRIPT_URL}..."
curl -fsSL -o "$TMP_SETUP" "$SETUP_SCRIPT_URL"
chmod +x "$TMP_SETUP"

MISSING_HOMES=()
FAILED_HOMES=()
FAILED_HOSTS=()

for HOME_DIR in "${TARGET_HOMES[@]}"; do
    echo "--------------------------------------------"
    echo "INFO    [qq metacentrum installer] Linking qq to ${HOME_DIR}..."
    if [ ! -d "$HOME_DIR" ]; then
        echo "WARN    [qq metacentrum installer] Skipping ${HOME_DIR} (directory not found)"
        MISSING_HOMES+=("$HOME_DIR")
        continue
    fi

    # run inside 'if' so a non-zero exit does not abort the script under 'set -e'
    if "$TMP_SETUP" "$HOME_DIR" "${MAIN_HOME}/qq"; then
        echo "INFO    [qq metacentrum installer] Linked qq to ${HOME_DIR}"
    else
        SETUP_STATUS=$?
        echo "ERROR   [qq metacentrum installer] Linking qq to ${HOME_DIR} failed (exit code ${SETUP_STATUS}). Continuing."
        FAILED_HOMES+=("$HOME_DIR")
    fi
done

# install qq on computers with local home directories (samson)
for HOST in "${LOCAL_HOME_HOSTS[@]}"; do
    echo "INFO    [qq metacentrum installer] Installing qq on ${HOST}..."
    if ssh -o BatchMode=yes -o ConnectTimeout=20 "${HOST}" "exit" 2>/dev/null; then
        if ssh "${HOST}" "curl -fsSL ${INSTALL_SCRIPT_URL} | bash -s -- \${HOME} ${RELEASE_URL}"; then
            echo "INFO    [qq metacentrum installer] Installation completed on ${HOST}"
        else
            echo "ERROR   [qq metacentrum installer] Installation on ${HOST} failed. Continuing."
            FAILED_HOSTS+=("$HOST")
        fi
    else
        echo "WARN    [qq metacentrum installer] Could not connect to ${HOST}. Skipping."
        FAILED_HOSTS+=("$HOST")
    fi
    echo "--------------------------------------------"
done

echo "--------------------------------------------"

if [ ${#MISSING_HOMES[@]} -gt 0 ]; then
    echo "WARN    [qq metacentrum installer] Home directories not found (skipped):"
    for MISSING in "${MISSING_HOMES[@]}"; do
        echo "WARN    [qq metacentrum installer]   - $MISSING"
    done
fi

if [ ${#FAILED_HOMES[@]} -gt 0 ]; then
    echo "ERROR   [qq metacentrum installer] Linking qq failed for the following home directories:"
    for FAILED in "${FAILED_HOMES[@]}"; do
        echo "ERROR   [qq metacentrum installer]   - $FAILED"
    done
fi

if [ ${#FAILED_HOSTS[@]} -gt 0 ]; then
    echo "ERROR   [qq metacentrum installer] Installation failed on the following hosts:"
    for FAILED in "${FAILED_HOSTS[@]}"; do
        echo "ERROR   [qq metacentrum installer]   - $FAILED"
    done
fi

if [ ${#MISSING_HOMES[@]} -eq 0 ] && [ ${#FAILED_HOMES[@]} -eq 0 ] && [ ${#FAILED_HOSTS[@]} -eq 0 ]; then
    echo "INFO    [qq metacentrum installer] qq installation completed for all target home directories."
fi

if ! contains "$HOME" ${MISSING_HOMES[@]+"${MISSING_HOMES[@]}"} ${FAILED_HOMES[@]+"${FAILED_HOMES[@]}"}; then
    echo "INFO    [qq metacentrum installer] Run 'source ${HOME}/.bashrc' to make qq available on the current machine."
fi

# cleanup
rm -f "$TMP_INSTALLER" "$TMP_SETUP"

# non-zero exit if anything went wrong (drop this if you always want a clean exit)
if [ ${#FAILED_HOMES[@]} -gt 0 ] || [ ${#FAILED_HOSTS[@]} -gt 0 ]; then
    exit 1
fi
