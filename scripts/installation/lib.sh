# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab
#
# Shared logic of the qq installers and uninstallers.
#
# This file is not run directly. `build.py` combines it with a cluster
# configuration from `clusters/` into a self-contained script.
#
# A cluster configuration defines:
#   QQ_CLUSTER        name of the cluster used in messages
#   QQ_HOMES          home directories to set up
#   QQ_SHARED_BUNDLE  "true": qq is installed only into QQ_MAIN_HOME and the other
#                     homes use it from there (copied to the local scratch on start);
#                     "false": qq is installed into each home separately
#   QQ_MAIN_HOME      home directory holding the shared qq (only if QQ_SHARED_BUNDLE)
#   QQ_SSH_HOSTS      machines with their own home directory, on which the
#                     generic installer is run over SSH
#
# Testing hooks (environment variables):
#   QQ_RELEASE_BASE_URL  where to download the release from
#   QQ_SCRATCH_DIR       scratch directory to copy the shared qq into

# shellcheck shell=bash

QQ_RELEASE_BASE_URL="${QQ_RELEASE_BASE_URL:-https://github.com/VachaLab/qq/releases/download/v${QQ_VERSION}}"
if [ -z "${QQ_SCRATCH_DIR:-}" ]; then
    # USER is expanded when a shell starts, on the machine it starts on
    # shellcheck disable=SC2016
    QQ_SCRATCH_DIR='/scratch/${USER}/qq'
fi

QQ_BLOCK_START="# >>> This block is managed by qq >>>"
QQ_BLOCK_END="# <<< This block is managed by qq <<<"

# content of .profile/.bash_profile created by installers older than qq v0.14
QQ_LEGACY_PROFILE='# Load .bashrc for login shells
if [ -f ~/.bashrc ]; then
. ~/.bashrc
fi'

QQ_WITH_ZSH=false
QQ_WITH_FISH=false
QQ_TMP=""
QQ_RESULTS=""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

qq_log() {
    printf '%-7s [qq %s] %s\n' "$1" "$QQ_CLUSTER" "$2"
}

qq_info() { qq_log INFO "$1"; }
qq_warn() { qq_log WARN "$1"; }
qq_error() { qq_log ERROR "$1" >&2; }

qq_die() {
    qq_error "$1"
    exit 1
}

# ---------------------------------------------------------------------------
# Command line and setup
# ---------------------------------------------------------------------------

qq_usage() {
    local action="$1"
    if [ "$action" = install ]; then
        echo "Usage: qq-install.sh [--zsh] [--fish]"
        echo
        echo "Installs qq ${QQ_VERSION} and sets it up for bash."
        echo "  --zsh   also set up qq for zsh"
        echo "  --fish  also set up qq for fish"
    else
        echo "Usage: qq-uninstall.sh"
        echo
        echo "Removes qq and all changes made by the qq installer."
    fi
}

qq_parse_args() {
    local action="$1"
    shift
    local arg
    for arg in "$@"; do
        case "${action}:${arg}" in
        install:--zsh) QQ_WITH_ZSH=true ;;
        install:--fish) QQ_WITH_FISH=true ;;
        *:-h | *:--help)
            qq_usage "$action"
            exit 0
            ;;
        *) qq_die "Unknown option '${arg}'. Use --help to show the usage." ;;
        esac
    done
}

qq_setup_tmp() {
    QQ_TMP="$(mktemp -d)"
    QQ_RESULTS="${QQ_TMP}/results"
    : >"$QQ_RESULTS"
    trap 'rm -rf "$QQ_TMP"' EXIT
}

# print the home directories to process, without duplicates, the main home first
qq_unique_homes() {
    local -A seen=()
    local home
    for home in ${QQ_MAIN_HOME:+"$QQ_MAIN_HOME"} "${QQ_HOMES[@]}"; do
        if [ -z "${seen[$home]:-}" ]; then
            seen[$home]=1
            printf '%s\n' "$home"
        fi
    done
}

qq_result() {
    printf '%s\t%s\t%s\n' "$1" "$2" "$3" >>"$QQ_RESULTS"
}

# ---------------------------------------------------------------------------
# qq bundle
# ---------------------------------------------------------------------------

# succeed if the directory contains a qq installation
qq_is_bundle() {
    [ -f "$1/VERSION" ] && [ -x "$1/qq" ] && [ -d "$1/_internal" ]
}

# download and unpack the release; prints the path to the unpacked qq
qq_download_release() {
    local archive="${QQ_TMP}/qq-release.tar.gz"
    local url="${QQ_RELEASE_BASE_URL}/qq-release.tar.gz"

    qq_info "Downloading qq ${QQ_VERSION} from ${url}..." >&2
    curl -fsSL -o "$archive" "$url" || qq_die "Could not download qq from ${url}."
    tar -xzf "$archive" -C "$QQ_TMP" || qq_die "Could not unpack the qq release."

    local bundle="${QQ_TMP}/qq"
    qq_is_bundle "$bundle" || qq_die "The downloaded release does not contain qq."

    local version
    version="$(cat "${bundle}/VERSION")"
    if [ "$version" != "$QQ_VERSION" ]; then
        qq_die "The downloaded release contains qq ${version}, but this installer is for qq ${QQ_VERSION}."
    fi

    printf '%s\n' "$bundle"
}

# install the qq bundle into a directory
# the old version is replaced only after the new one has been fully copied
qq_install_bundle() {
    local source="$1" dest="$2"

    if qq_is_bundle "$dest" && [ "$(cat "${dest}/VERSION")" = "$QQ_VERSION" ]; then
        qq_info "qq ${QQ_VERSION} is already installed in ${dest}."
        return 0
    fi

    mkdir -p "$(dirname "$dest")"
    rm -rf "${dest}.new" "${dest}.old"
    cp -R "$source" "${dest}.new"
    if [ -e "$dest" ]; then
        mv "$dest" "${dest}.old"
    fi
    mv "${dest}.new" "$dest"
    rm -rf "${dest}.old"
    qq_info "Installed qq ${QQ_VERSION} into ${dest}."
}

# ---------------------------------------------------------------------------
# Shell configuration files
# ---------------------------------------------------------------------------

# print the shell code that puts qq on PATH
# arguments: bundle directory, "true" to copy qq to the scratch directory
qq_env_path_posix() {
    local bundle="$1" scratch="$2"
    cat <<EOF
if [[ ":\${PATH}:" != *":${bundle}:"* ]]; then
    export PATH="\${PATH}:${bundle}"
fi
EOF
    if [ "$scratch" = true ]; then
        cat <<EOF

# Copy qq to the scratch directory of this machine, where it starts faster.
if [ -d "${bundle}" ] && rsync -a --delete --quiet "${bundle}/" "${QQ_SCRATCH_DIR}/" 2>/dev/null; then
    if [[ ":\${PATH}:" != *":${QQ_SCRATCH_DIR}:"* ]]; then
        export PATH="${QQ_SCRATCH_DIR}:\${PATH}"
    fi
fi
EOF
    fi
}

# print the `qq` shell function which makes `qq cd` change the directory
qq_env_function_posix() {
    cat <<'EOF'
# Make 'qq cd' change the directory.
qq() {
    if [[ "${1:-}" == "cd" ]]; then
        local arg target_dir
        for arg in "$@"; do
            if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
                command qq "$@"
                return
            fi
        done
        target_dir="$(command qq cd "${@:2}")" || return
        cd "$target_dir" || return
    else
        command qq "$@"
    fi
}
EOF
}

# print the header of an env file which also prevents loading it twice
qq_env_header() {
    echo "# This file is managed by the qq installer (qq ${QQ_VERSION}). Do not edit it."
    cat <<'EOF'

if [ -n "${__QQ_ENV_LOADED:-}" ]; then return 0; fi
__QQ_ENV_LOADED=1

EOF
}

qq_write_env_bash() {
    local file="$1" bundle="$2" scratch="$3"
    {
        qq_env_header
        qq_env_path_posix "$bundle" "$scratch"
        echo
        qq_env_function_posix
        cat <<'EOF'

# Enable tab completion in interactive shells.
if [[ $- == *i* ]]; then
    eval "$(_QQ_COMPLETE=bash_source command qq)"
fi
EOF
    } >"$file"
}

# write the file loaded by every zsh, including non-interactive shells
# started by SSH; it only puts qq on PATH
qq_write_path_zsh() {
    local file="$1" bundle="$2"
    {
        echo "# This file is managed by the qq installer (qq ${QQ_VERSION}). Do not edit it."
        echo
        qq_env_path_posix "$bundle" false
    } >"$file"
}

qq_write_env_zsh() {
    local file="$1" bundle="$2" scratch="$3"
    {
        qq_env_header
        qq_env_path_posix "$bundle" "$scratch"
        echo
        qq_env_function_posix
        cat <<'EOF'

# Enable tab completion in interactive shells.
if [[ -o interactive ]]; then
    (( ${+functions[compdef]} )) || { autoload -Uz compinit && compinit -i; }
    eval "$(_QQ_COMPLETE=zsh_source command qq)"
fi
EOF
    } >"$file"
}

qq_write_env_fish() {
    local file="$1" bundle="$2" scratch="$3"
    mkdir -p "$(dirname "$file")"
    {
        cat <<EOF
# This file is managed by the qq installer (qq ${QQ_VERSION}). Do not edit it.

if not contains -- "${bundle}" \$PATH
    set -gx PATH \$PATH "${bundle}"
end
EOF
        if [ "$scratch" = true ]; then
            # fish does not understand the ${USER} syntax
            local scratch_dir="${QQ_SCRATCH_DIR//\$\{USER\}/\$USER}"
            cat <<EOF

# Copy qq to the scratch directory of this machine, where it starts faster.
if test -d "${bundle}"; and rsync -a --delete --quiet "${bundle}/" "${scratch_dir}/" 2>/dev/null
    if not contains -- "${scratch_dir}" \$PATH
        set -gx PATH "${scratch_dir}" \$PATH
    end
end
EOF
        fi
        cat <<EOF

# Make 'qq cd' change the directory.
function qq
    if test (count \$argv) -ge 1; and test "\$argv[1]" = cd
        if contains -- --help \$argv; or contains -- -h \$argv
            command qq \$argv
            return
        end
        set -l target_dir (command qq cd \$argv[2..-1]); or return
        cd \$target_dir
    else
        command qq \$argv
    end
end

# Enable tab completion in interactive shells.
if status is-interactive
    env _QQ_COMPLETE=fish_source qq | source
end
EOF
    } >"$file"
}

# succeed if the qq block markers in the file are balanced
qq_blocks_valid() {
    local file="$1" starts ends
    [ -f "$file" ] || return 0
    starts="$(grep -c '^# >>> This block is managed by qq' "$file" || true)"
    ends="$(grep -c '^# <<< This block is managed by qq' "$file" || true)"
    [ "$starts" = "$ends" ]
}

# rewrite a file without qq blocks (current and legacy ones) and, if requested, with a new qq block
# one blank line before and after each removed block is removed as well, since the installers add them
# arguments: file, block content (empty to only remove), position (top or bottom)
qq_rewrite_blocks() {
    local file="$1" block="$2" position="$3"

    if ! qq_blocks_valid "$file"; then
        qq_error "${file} contains an incomplete qq block. Please fix it manually."
        return 1
    fi

    local tmp="${QQ_TMP}/rewrite"
    if [ -f "$file" ]; then
        cp "$file" "$tmp.in"
    else
        : >"$tmp.in"
    fi

    QQ_BLOCK="$block" QQ_POSITION="$position" awk '
        BEGIN { block = ENVIRON["QQ_BLOCK"]; position = ENVIRON["QQ_POSITION"]; n = 0 }
        /^# >>> This block is managed by qq/ {
            inside = 1
            if (n > 0 && kept[n] ~ /^[[:space:]]*$/) n--
            next
        }
        inside && /^# <<< This block is managed by qq/ { inside = 0; skip_blank = 1; next }
        inside { next }
        skip_blank && /^[[:space:]]*$/ { skip_blank = 0; next }
        { skip_blank = 0; kept[++n] = $0 }
        END {
            if (block == "") {
                for (i = 1; i <= n; i++) print kept[i]
            } else if (position == "top") {
                first = 1
                if (n >= 1 && kept[1] ~ /^#!/) { print kept[1]; first = 2 }
                print block
                if (first <= n) {
                    print ""
                    for (i = first; i <= n; i++) print kept[i]
                }
            } else {
                for (i = 1; i <= n; i++) print kept[i]
                if (n > 0) print ""
                print block
            }
        }
    ' "$tmp.in" >"$tmp.out"

    # write through the existing file to keep symlinks and permissions
    if [ ! -f "$file" ] || ! cmp -s "$tmp.out" "$file"; then
        cat "$tmp.out" >"$file"
    fi
}

qq_block() {
    printf '%s\n%s\n%s' "$QQ_BLOCK_START" "$1" "$QQ_BLOCK_END"
}

# make all shells of a home directory load qq
qq_configure_shells() {
    local home="$1" bundle="$2" scratch="$3"
    local qq_dir="${home}/.qq"

    mkdir -p "$qq_dir"
    qq_write_env_bash "${qq_dir}/env.bash" "$bundle" "$scratch"

    # shellcheck disable=SC2016
    local load_bash='if [ -f "$HOME/.qq/env.bash" ]; then . "$HOME/.qq/env.bash"; fi'
    # .profile is also read by other shells than bash
    # shellcheck disable=SC2016
    local load_profile='if [ -n "${BASH_VERSION:-}" ] && [ -f "$HOME/.qq/env.bash" ]; then . "$HOME/.qq/env.bash"; fi'

    # the block is placed at the top so that it runs before any early return
    # for non-interactive shells
    qq_rewrite_blocks "${home}/.bashrc" "$(qq_block "$load_bash")" top
    qq_rewrite_blocks "${home}/.profile" "$(qq_block "$load_profile")" top
    # bash login shells read only the first of .bash_profile, .bash_login
    # and .profile; creating one of the former would hide .profile
    local file
    for file in .bash_profile .bash_login; do
        if [ -f "${home}/${file}" ]; then
            qq_rewrite_blocks "${home}/${file}" "$(qq_block "$load_bash")" top
        fi
    done

    if [ "$QQ_WITH_ZSH" = true ]; then
        qq_write_path_zsh "${qq_dir}/path.zsh" "$bundle"
        qq_write_env_zsh "${qq_dir}/env.zsh" "$bundle" "$scratch"
        # .zshenv is read by every zsh, including commands run over SSH; the
        # block goes to the end so that the user's own PATH settings do not remove qq
        # shellcheck disable=SC2016
        local load_zsh_path='if [ -f "$HOME/.qq/path.zsh" ]; then . "$HOME/.qq/path.zsh"; fi'
        qq_rewrite_blocks "${home}/.zshenv" "$(qq_block "$load_zsh_path")" bottom
        # .zshrc is only read by interactive shells; the block goes to the end,
        # after the user's own completion setup
        # shellcheck disable=SC2016
        local load_zsh='if [ -f "$HOME/.qq/env.zsh" ]; then . "$HOME/.qq/env.zsh"; fi'
        qq_rewrite_blocks "${home}/.zshrc" "$(qq_block "$load_zsh")" bottom
    fi

    if [ "$QQ_WITH_FISH" = true ]; then
        qq_write_env_fish "${home}/.config/fish/conf.d/qq.fish" "$bundle" "$scratch"
    fi

    local shells="bash"
    if [ "$QQ_WITH_ZSH" = true ]; then shells+=", zsh"; fi
    if [ "$QQ_WITH_FISH" = true ]; then shells+=", fish"; fi
    qq_info "Set up qq for ${shells} in ${home}."
}

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

# run a command in a clean environment with the given home directory
# the command runs in a new session without a terminal; otherwise interactive
# shells would try to take over the terminal and stop while waiting for it
qq_run_clean() {
    local home="$1"
    shift
    local runner=()
    if command -v setsid >/dev/null; then
        runner+=(setsid)
        # without setsid, timeout would move the shell away from the terminal
        if command -v timeout >/dev/null; then
            runner+=(timeout 120)
        fi
    fi
    env -i HOME="$home" USER="$USER" LOGNAME="$USER" TERM=dumb \
        PATH="/usr/local/bin:/usr/bin:/bin" ${runner[@]+"${runner[@]}"} "$@" </dev/null 2>/dev/null
}

# run one check and record its result
# arguments: home, check name, expected output line, command...
qq_check() {
    local home="$1" name="$2" expected="$3"
    shift 3
    local output
    output="$(qq_run_clean "$home" "$@" || true)"
    if printf '%s\n' "$output" | grep -qxF -- "$expected"; then
        qq_result "$home" "$name" OK
        return 0
    fi
    qq_result "$home" "$name" FAIL
    return 1
}

# check that qq is available in all set-up shells of a home directory
qq_check_home() {
    local home="$1" bundle="$2" expected status=0
    expected="$("${bundle}/qq" --version </dev/null 2>/dev/null || true)"
    if [ -z "$expected" ]; then
        qq_result "$home" "qq --version" FAIL
        return 1
    fi

    # PBS and Slurm jobs and login shells
    qq_check "$home" "bash (login)" "$expected" bash -lc 'qq --version' || status=1
    # commands run over ssh: non-interactive shells reading .bashrc
    # shellcheck disable=SC2016
    qq_check "$home" "bash (non-interactive)" "$expected" \
        bash -c '. "$HOME/.bashrc" >/dev/null 2>&1; qq --version' || status=1
    # terminals, including the `qq cd` function
    qq_check "$home" "bash (interactive)" "$expected" \
        bash -ic 'declare -F qq >/dev/null && qq --version' || status=1

    if [ "$QQ_WITH_ZSH" = true ]; then
        if command -v zsh >/dev/null; then
            # commands run over ssh: non-interactive shells reading .zshenv
            qq_check "$home" "zsh (non-interactive)" "$expected" \
                zsh -c 'qq --version' || status=1
            qq_check "$home" "zsh (interactive)" "$expected" \
                zsh -ic 'whence -w qq | grep -q function && qq --version' || status=1
        else
            qq_result "$home" "zsh" "SKIPPED (zsh not available here)"
        fi
    fi

    if [ "$QQ_WITH_FISH" = true ]; then
        if command -v fish >/dev/null; then
            qq_check "$home" "fish" "$expected" \
                fish -c 'functions -q qq; and qq --version' || status=1
        else
            qq_result "$home" "fish" "SKIPPED (fish not available here)"
        fi
    fi

    return "$status"
}

# ---------------------------------------------------------------------------
# Removal
# ---------------------------------------------------------------------------

# remove the qq directory used by installers older than qq v0.14
qq_remove_legacy() {
    local legacy="$1/qq"
    [ -e "$legacy" ] || return 0
    if qq_is_bundle "$legacy"; then
        rm -rf "$legacy"
        qq_info "Removed the old qq installation ${legacy}."
    else
        qq_warn "${legacy} does not look like a qq installation. Leaving it in place."
    fi
}

# remove qq blocks from a shell configuration file
# files the installers may have created are deleted if nothing else remains in them
# .bash_profile is only deleted if it contains nothing but what old installers created, and
# .bash_login is never deleted, since removing them changes which file bash reads.
qq_clean_rc_file() {
    local file="$1"
    [ -f "$file" ] || return 0

    if grep -q '^# >>> This block is managed by qq' "$file"; then
        qq_rewrite_blocks "$file" "" top || return 1
        qq_info "Removed the qq block from ${file}."
    fi

    local content
    content="$(grep -v '^[[:space:]]*$' "$file" | sed 's/^[[:space:]]*//' || true)"
    case "$file" in
    */.profile)
        if [ -z "$content" ] || [ "$content" = "$QQ_LEGACY_PROFILE" ]; then
            rm -f "$file"
            qq_info "Removed ${file} created by the qq installer."
        fi
        ;;
    */.bash_profile)
        if [ "$content" = "$QQ_LEGACY_PROFILE" ]; then
            rm -f "$file"
            qq_info "Removed ${file} created by the qq installer."
        fi
        ;;
    */.bash_login) ;;
    *)
        if [ -z "$content" ]; then
            rm -f "$file"
            qq_info "Removed the empty ${file}."
        fi
        ;;
    esac
}

qq_uninstall_home() {
    local home="$1" status=0 file

    for file in .bashrc .bash_profile .bash_login .profile .zshenv .zshrc; do
        qq_clean_rc_file "${home}/${file}" || status=1
    done

    if [ -f "${home}/.config/fish/conf.d/qq.fish" ]; then
        rm -f "${home}/.config/fish/conf.d/qq.fish"
        qq_info "Removed ${home}/.config/fish/conf.d/qq.fish."
    fi

    if [ -e "${home}/.qq" ]; then
        rm -rf "${home}/.qq"
        qq_info "Removed ${home}/.qq."
    fi

    qq_remove_legacy "$home"
    return "$status"
}

# ---------------------------------------------------------------------------
# Machines with their own home directories
# ---------------------------------------------------------------------------

# run the generic installer or uninstaller on a machine over SSH
qq_run_on_host() {
    local host="$1" script="$2"
    shift 2
    local url="${QQ_RELEASE_BASE_URL}/${script}"

    qq_info "Running ${script} on ${host}..."
    if ! ssh -o BatchMode=yes -o ConnectTimeout=20 "$host" true </dev/null 2>/dev/null; then
        qq_warn "Could not connect to ${host}."
        qq_result "$host" "remote ${script}" "FAIL (could not connect)"
        return 1
    fi
    # shellcheck disable=SC2029
    if ssh -o BatchMode=yes "$host" "curl -fsSL '${url}' | bash -s -- $*" </dev/null; then
        qq_result "$host" "remote ${script}" OK
        return 0
    fi
    qq_result "$host" "remote ${script}" FAIL
    return 1
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

qq_print_summary() {
    echo "--------------------------------------------"
    qq_info "Summary:"
    local where name status detail
    while IFS=$'\t' read -r where name status; do
        detail=""
        if [ "$status" != "${status%% *}" ]; then
            detail=" ${status#* }"
        fi
        printf '  %-8s %-24s %s%s\n' "${status%% *}" "$name" "$where" "$detail"
    done <"$QQ_RESULTS"
    echo "--------------------------------------------"
}

# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

# istall qq into one home directory and check it; run in a subshell
qq_install_home() {
    local home="$1" source_bundle="$2" bundle
    qq_info "Setting up ${home}..."

    if [ "$QQ_SHARED_BUNDLE" = true ]; then
        bundle="${QQ_MAIN_HOME}/.qq/qq"
        if [ "$home" = "$QQ_MAIN_HOME" ]; then
            qq_install_bundle "$source_bundle" "$bundle"
        fi
    else
        bundle="${home}/.qq/qq"
        qq_install_bundle "$source_bundle" "$bundle"
    fi

    qq_configure_shells "$home" "$bundle" "$QQ_SHARED_BUNDLE"

    if qq_check_home "$home" "$bundle"; then
        qq_remove_legacy "$home"
    else
        qq_error "qq is not available in all shells of ${home}. See the summary below."
        return 1
    fi
}

qq_install() {
    qq_parse_args install "$@"
    qq_setup_tmp

    local source_bundle
    source_bundle="$(qq_download_release)"

    if [ "$QQ_SHARED_BUNDLE" = true ] && [ ! -d "$QQ_MAIN_HOME" ]; then
        qq_die "The main home directory ${QQ_MAIN_HOME} is not available."
    fi

    local failed=false home status homes
    # the installer may itself be read from the standard input, so nothing may read it
    mapfile -t homes < <(qq_unique_homes)
    for home in "${homes[@]}"; do
        echo "--------------------------------------------"
        if [ ! -d "$home" ]; then
            qq_warn "Skipping ${home} (directory not found)."
            qq_result "$home" "home directory" "SKIPPED (not found)"
            continue
        fi
        set +e
        (
            set -e
            qq_install_home "$home" "$source_bundle"
        )
        status=$?
        set -e
        if [ "$status" -ne 0 ]; then
            failed=true
            qq_result "$home" "installation" FAIL
        fi
    done

    local flags=()
    if [ "$QQ_WITH_ZSH" = true ]; then flags+=(--zsh); fi
    if [ "$QQ_WITH_FISH" = true ]; then flags+=(--fish); fi
    local host
    for host in ${QQ_SSH_HOSTS[@]+"${QQ_SSH_HOSTS[@]}"}; do
        echo "--------------------------------------------"
        qq_run_on_host "$host" qq-install.sh ${flags[@]+"${flags[@]}"} || failed=true
    done

    qq_print_summary
    if [ "$failed" = true ]; then
        qq_error "qq ${QQ_VERSION} was not installed everywhere."
        exit 1
    fi
    qq_info "qq ${QQ_VERSION} is installed. Open a new terminal to start using it."
}

qq_uninstall() {
    qq_parse_args uninstall "$@"
    qq_setup_tmp

    local failed=false home status homes
    # the installer may itself be read from the standard input, so nothing may read it
    mapfile -t homes < <(qq_unique_homes)
    for home in "${homes[@]}"; do
        echo "--------------------------------------------"
        if [ ! -d "$home" ]; then
            qq_warn "Skipping ${home} (directory not found)."
            continue
        fi
        qq_info "Removing qq from ${home}..."
        set +e
        (
            set -e
            qq_uninstall_home "$home"
        )
        status=$?
        set -e
        if [ "$status" -eq 0 ]; then
            qq_result "$home" "uninstallation" OK
        else
            failed=true
            qq_result "$home" "uninstallation" FAIL
        fi
    done

    if [ "$QQ_SHARED_BUNDLE" = true ]; then
        # only the scratch directory of this machine can be reached
        local scratch
        scratch="$(eval "printf '%s' \"${QQ_SCRATCH_DIR}\"")"
        if qq_is_bundle "$scratch"; then
            rm -rf "$scratch"
            qq_info "Removed ${scratch}. Copies on the scratch of other machines are no longer used."
        fi
    fi

    local host
    for host in ${QQ_SSH_HOSTS[@]+"${QQ_SSH_HOSTS[@]}"}; do
        echo "--------------------------------------------"
        qq_run_on_host "$host" qq-uninstall.sh || failed=true
    done

    qq_print_summary
    if [ "$failed" = true ]; then
        qq_error "qq was not removed everywhere."
        exit 1
    fi
    qq_info "qq has been removed. Open a new terminal for the change to take effect."
}
