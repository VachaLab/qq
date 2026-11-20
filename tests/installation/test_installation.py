# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

import contextlib
import importlib.util
import os
import pty
import select
import shutil
import signal
import subprocess
import tarfile
import time
from pathlib import Path

import pytest

SOURCE_DIR = Path(__file__).resolve().parents[2] / "scripts" / "installation"

_spec = importlib.util.spec_from_file_location(
    "qq_installation_build", SOURCE_DIR / "build.py"
)
assert _spec is not None and _spec.loader is not None
build = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(build)

VERSION = "1.2.3"
CLEAN_PATH = "/usr/local/bin:/usr/bin:/bin"
START = "# >>> This block is managed by qq >>>"
END = "# <<< This block is managed by qq <<<"

pytestmark = pytest.mark.skipif(
    shutil.which("curl") is None, reason="the installers require curl"
)

needs_zsh = pytest.mark.skipif(
    shutil.which("zsh") is None, reason="zsh is not installed"
)
needs_fish = pytest.mark.skipif(
    shutil.which("fish") is None, reason="fish is not installed"
)
needs_rsync = pytest.mark.skipif(
    shutil.which("rsync") is None, reason="rsync is not installed"
)

# A stand-in for the qq executable. It prints the version from the VERSION file,
# the path given to `qq cd`, and its own location for `qq where`.
FAKE_QQ = """#!/bin/bash
here="$(cd "$(dirname "$0")" && pwd)"
if [ -n "${_QQ_COMPLETE:-}" ]; then
    echo "# completion: ${_QQ_COMPLETE}"
    exit 0
fi
case "${1:-}" in
--version) cat "${here}/VERSION" ;;
cd) printf '%s\\n' "$2" ;;
where) printf '%s\\n' "$here" ;;
*) echo "qq $*" ;;
esac
"""

LEGACY_BLOCK = """# >>> This block is managed by qq (v0.13.0) >>>
# This makes qq available for you on any computer using this directory as its HOME.
if [[ ":$PATH:" != *":{home}/qq:"* ]]; then
    export PATH="$PATH:{home}/qq"
fi
eval "$(_QQ_COMPLETE=bash_source qq)"
# <<< This block is managed by qq (v0.13.0) <<<
"""

LEGACY_PROFILE = """
# Load .bashrc for login shells
if [ -f ~/.bashrc ]; then
    . ~/.bashrc
fi
"""


def make_release(directory: Path, version: str = VERSION) -> str:
    """Create a fake qq release and return its base URL."""
    bundle = directory / "build" / "qq"
    (bundle / "_internal").mkdir(parents=True)
    (bundle / "VERSION").write_text(version + "\n")
    (bundle / "qq").write_text(FAKE_QQ)
    (bundle / "qq").chmod(0o755)

    with tarfile.open(directory / "qq-release.tar.gz", "w:gz") as archive:
        archive.add(bundle, arcname="qq")
    return f"file://{directory}"


def make_bundle(path: Path, version: str = "0.13.0") -> None:
    """Create a fake qq installation directory."""
    (path / "_internal").mkdir(parents=True)
    (path / "VERSION").write_text(version + "\n")
    (path / "qq").write_text(FAKE_QQ)
    (path / "qq").chmod(0o755)


def make_scripts(
    tmp_path: Path,
    homes: list[Path],
    shared: bool = False,
    main_home: Path | None = None,
    version: str = VERSION,
) -> tuple[Path, Path]:
    """Build an installer and an uninstaller for a test cluster."""
    main = main_home or homes[0]
    config = tmp_path / "test.sh"
    config.write_text(
        'QQ_CLUSTER="test"\n'
        f'QQ_MAIN_HOME="{main}"\n'
        "QQ_HOMES=(" + " ".join(f'"{home}"' for home in homes) + ")\n"
        f"QQ_SHARED_BUNDLE={'true' if shared else 'false'}\n"
        "QQ_SSH_HOSTS=()\n"
    )
    scripts = tmp_path / f"scripts-{version}"
    scripts.mkdir(exist_ok=True)
    installer = scripts / "install.sh"
    uninstaller = scripts / "uninstall.sh"
    installer.write_text(build.build_script(config, "install", version))
    uninstaller.write_text(build.build_script(config, "uninstall", version))
    return installer, uninstaller


def run_script(
    script: Path, home: Path, url: str, *args: str, scratch: Path | None = None
) -> subprocess.CompletedProcess[str]:
    env = {
        "HOME": str(home),
        "USER": "tester",
        "PATH": os.environ["PATH"],
        "QQ_RELEASE_BASE_URL": url,
    }
    if scratch is not None:
        env["QQ_SCRATCH_DIR"] = str(scratch)
    return subprocess.run(
        ["bash", str(script), *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        timeout=300,
    )


def run_shell(home: Path, *command: str) -> str:
    """Run a shell in a clean environment like a new session of the user."""
    result = subprocess.run(
        list(command),
        env={"HOME": str(home), "USER": "tester", "PATH": CLEAN_PATH, "TERM": "dumb"},
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        timeout=60,
    )
    return result.stdout


def run_in_terminal(
    command: list[str], env: dict[str, str], timeout: float
) -> int | None:
    """
    Run a command with a terminal as its controlling terminal.

    Returns the exit code, or None if the command did not finish in time.
    """
    pid, fd = pty.fork()
    if pid == 0:
        os.execvpe(command[0], command, env)

    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            # the output has to be read, otherwise the command blocks on writing
            ready, _, _ = select.select([fd], [], [], 0.2)
            if ready:
                with contextlib.suppress(OSError):
                    os.read(fd, 4096)
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                return os.waitstatus_to_exitcode(status)
        os.killpg(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        return None
    finally:
        os.close(fd)


def block_count(text: str) -> int:
    return text.count("# >>> This block is managed by qq")


@pytest.fixture
def home(tmp_path: Path) -> Path:
    path = tmp_path / "home"
    path.mkdir()
    return path


@pytest.fixture
def url(tmp_path: Path) -> str:
    release = tmp_path / "release"
    release.mkdir()
    return make_release(release)


def test_install_fresh_home(tmp_path, home, url):
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (home / ".qq" / "qq" / "VERSION").read_text() == f"{VERSION}\n"
    assert (home / ".qq" / "env.bash").is_file()
    for name in (".bashrc", ".profile"):
        content = (home / name).read_text()
        assert content.startswith(START)
        assert block_count(content) == 1
    assert not (home / ".bash_profile").exists()
    assert not (home / ".bash_login").exists()
    assert "BASH_VERSION" in (home / ".profile").read_text()
    assert not (home / ".zshrc").exists()
    assert not (home / ".zshenv").exists()
    assert not (home / ".config" / "fish").exists()
    assert "OK" in result.stdout


@pytest.mark.parametrize(
    ("command", "expected"),
    [
        (["bash", "-lc", "qq --version"], VERSION),
        (["bash", "-c", '. "$HOME/.bashrc"; qq --version'], VERSION),
        (["bash", "-ic", "qq --version"], VERSION),
        (["bash", "-ic", "type -t qq"], "function"),
        (["sh", "-lc", "echo ok"], "ok"),
    ],
)
def test_install_makes_qq_available(tmp_path, home, url, command, expected):
    installer, _ = make_scripts(tmp_path, [home])
    assert run_script(installer, home, url).returncode == 0

    assert run_shell(home, *command).strip() == expected


def test_install_qq_cd_changes_directory(tmp_path, home, url):
    installer, _ = make_scripts(tmp_path, [home])
    assert run_script(installer, home, url).returncode == 0
    target = tmp_path / "job"
    target.mkdir()

    output = run_shell(home, "bash", "-ic", f"qq cd {target} && pwd")

    assert output.strip() == str(target)


@pytest.mark.parametrize(
    "existing",
    [[".bash_profile"], [".bash_login"], [".bash_profile", ".bash_login"]],
)
def test_install_modifies_existing_bash_login_files(tmp_path, home, url, existing):
    for name in existing:
        (home / name).write_text("export LOGIN=1\n")
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    for name in (".bash_profile", ".bash_login"):
        if name in existing:
            content = (home / name).read_text()
            assert content.startswith(START)
            assert content.endswith(f"{END}\n\nexport LOGIN=1\n")
        else:
            assert not (home / name).exists()
    # .profile is created even though bash does not read it here
    assert (home / ".profile").read_text().startswith(START)
    output = run_shell(home, "bash", "-lc", "echo $LOGIN; qq --version")
    assert output.split() == ["1", VERSION]


def test_install_keeps_profile_loaded_in_login_shells(tmp_path, home, url):
    (home / ".profile").write_text("export PROFILE=1\n")
    installer, _ = make_scripts(tmp_path, [home])

    assert run_script(installer, home, url).returncode == 0

    assert not (home / ".bash_profile").exists()
    output = run_shell(home, "bash", "-lc", "echo $PROFILE; qq --version")
    assert output.split() == ["1", VERSION]


def test_install_places_block_after_shebang(tmp_path, home, url):
    (home / ".bashrc").write_text("#!/bin/bash\nalias ll='ls -l'\n")
    installer, _ = make_scripts(tmp_path, [home])

    assert run_script(installer, home, url).returncode == 0

    lines = (home / ".bashrc").read_text().splitlines()
    assert lines[0] == "#!/bin/bash"
    assert lines[1] == START
    assert lines[-1] == "alias ll='ls -l'"


def test_install_is_idempotent(tmp_path, home, url):
    (home / ".bashrc").write_text("export A=1\n")
    (home / ".profile").write_text("export B=2\n")
    (home / ".bash_profile").write_text("export C=3\n")
    (home / ".bash_login").write_text("export D=4\n")
    installer, _ = make_scripts(tmp_path, [home])

    assert run_script(installer, home, url).returncode == 0
    first = {
        name: (home / name).read_text()
        for name in (".bashrc", ".bash_profile", ".bash_login", ".profile")
    }
    assert run_script(installer, home, url).returncode == 0
    second = {
        name: (home / name).read_text()
        for name in (".bashrc", ".bash_profile", ".bash_login", ".profile")
    }

    assert first == second


def test_install_works_with_early_return_in_bashrc(tmp_path, home, url):
    (home / ".bashrc").write_text("case $- in\n    *i*) ;;\n    *) return ;;\nesac\n")
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    output = run_shell(home, "bash", "-c", '. "$HOME/.bashrc"; qq --version')
    assert output.strip() == VERSION


def test_install_keeps_symlinked_bashrc(tmp_path, home, url):
    real = tmp_path / "dotfiles" / "bashrc"
    real.parent.mkdir()
    real.write_text("export A=1\n")
    (home / ".bashrc").symlink_to(real)
    installer, _ = make_scripts(tmp_path, [home])

    assert run_script(installer, home, url).returncode == 0

    assert (home / ".bashrc").is_symlink()
    assert real.read_text().startswith(START)


def test_install_migrates_legacy_installation(tmp_path, home, url):
    make_bundle(home / "qq")
    (home / ".bashrc").write_text(
        "export A=1\n\n" + LEGACY_BLOCK.replace("{home}", str(home)) + "\n"
    )
    (home / ".profile").write_text(LEGACY_PROFILE)
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    bashrc = (home / ".bashrc").read_text()
    assert "v0.13.0" not in bashrc
    assert block_count(bashrc) == 1
    assert bashrc.startswith(START)
    assert bashrc.endswith(f"{END}\n\nexport A=1\n")
    assert (home / ".profile").read_text().endswith(LEGACY_PROFILE)
    assert not (home / "qq").exists()


def test_install_keeps_unrelated_qq_directory(tmp_path, home, url):
    (home / "qq").mkdir()
    (home / "qq" / "notes.txt").write_text("my own data\n")
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 0
    assert (home / "qq" / "notes.txt").is_file()
    assert "does not look like a qq installation" in result.stdout


def test_install_fails_on_incomplete_block(tmp_path, home, url):
    (home / ".bashrc").write_text(f"{START}\nexport A=1\n")
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 1
    assert "incomplete qq block" in result.stderr
    assert (home / ".bashrc").read_text() == f"{START}\nexport A=1\n"


def test_install_detects_qq_not_available(tmp_path, home, url):
    # PATH is reset after the qq block, so qq disappears in non-login shells
    (home / ".bashrc").write_text("export PATH=/usr/bin:/bin\n")
    make_bundle(home / "qq")
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 1
    assert "FAIL" in result.stdout
    # the old installation is kept if the new one does not work
    assert (home / "qq").is_dir()


def test_install_version_mismatch_changes_nothing(tmp_path, home):
    release = tmp_path / "release"
    release.mkdir()
    url = make_release(release, version="9.9.9")
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url)

    assert result.returncode == 1
    assert "contains qq 9.9.9" in result.stderr
    assert list(home.iterdir()) == []


def test_install_missing_release_fails(tmp_path, home):
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, f"file://{tmp_path}/nothing")

    assert result.returncode == 1
    assert "Could not download qq" in result.stderr
    assert list(home.iterdir()) == []


def test_install_skips_missing_home(tmp_path, home, url):
    missing = tmp_path / "missing"
    installer, _ = make_scripts(tmp_path, [home, missing])

    result = run_script(installer, home, url)

    assert result.returncode == 0
    assert f"Skipping {missing}" in result.stdout
    assert not missing.exists()
    assert (home / ".qq" / "qq").is_dir()


def test_install_multiple_homes_separate_bundles(tmp_path, home, url):
    other = tmp_path / "storage"
    other.mkdir()
    installer, _ = make_scripts(tmp_path, [home, other, home])

    result = run_script(installer, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count(f"Setting up {home}...") == 1
    for path in (home, other):
        assert (path / ".qq" / "qq").is_dir()
        assert str(path / ".qq" / "qq") in (path / ".qq" / "env.bash").read_text()
        assert run_shell(path, "bash", "-lc", "qq --version").strip() == VERSION


def test_install_updates_bundle(tmp_path, home, url):
    installer, _ = make_scripts(tmp_path, [home])
    assert run_script(installer, home, url).returncode == 0
    new_release = tmp_path / "release-new"
    new_release.mkdir()
    new_url = make_release(new_release, version="1.2.4")
    new_installer, _ = make_scripts(tmp_path, [home], version="1.2.4")

    result = run_script(new_installer, home, new_url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (home / ".qq" / "qq" / "VERSION").read_text() == "1.2.4\n"
    assert sorted(path.name for path in (home / ".qq").iterdir()) == ["env.bash", "qq"]
    assert "1.2.4" in (home / ".qq" / "env.bash").read_text()
    assert run_shell(home, "bash", "-lc", "qq --version").strip() == "1.2.4"


@needs_rsync
def test_install_shared_bundle(tmp_path, url):
    main = tmp_path / "main"
    other = tmp_path / "other"
    main.mkdir()
    other.mkdir()
    scratch = tmp_path / "scratch" / "qq"
    scratch.parent.mkdir()
    installer, uninstaller = make_scripts(
        tmp_path, [other], shared=True, main_home=main
    )

    result = run_script(installer, main, url, scratch=scratch)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (main / ".qq" / "qq").is_dir()
    assert not (other / ".qq" / "qq").exists()
    assert str(main / ".qq" / "qq") in (other / ".qq" / "env.bash").read_text()
    assert run_shell(other, "bash", "-lc", "qq where").strip() == str(scratch)

    result = run_script(uninstaller, main, url, scratch=scratch)

    assert result.returncode == 0, result.stdout + result.stderr
    assert not scratch.exists()
    assert not (main / ".qq").exists()
    assert not (other / ".qq").exists()


@needs_fish
@needs_rsync
def test_install_shared_bundle_fish(tmp_path, url):
    main = tmp_path / "main"
    other = tmp_path / "other"
    main.mkdir()
    other.mkdir()
    scratch = tmp_path / "scratch" / "qq"
    scratch.parent.mkdir()
    installer, _ = make_scripts(tmp_path, [other], shared=True, main_home=main)

    result = run_script(installer, main, url, "--fish", scratch=scratch)

    assert result.returncode == 0, result.stdout + result.stderr
    assert run_shell(other, "fish", "-c", "qq where").strip() == str(scratch)


@needs_fish
@needs_rsync
def test_install_shared_bundle_fish_without_scratch(tmp_path, url):
    main = tmp_path / "main"
    main.mkdir()
    scratch = tmp_path / "scratch" / "qq"
    installer, _ = make_scripts(tmp_path, [main], shared=True, main_home=main)

    result = run_script(installer, main, url, "--fish", scratch=scratch)

    assert result.returncode == 0, result.stdout + result.stderr
    assert run_shell(main, "fish", "-c", "qq where").strip() == str(main / ".qq" / "qq")


@needs_zsh
@needs_rsync
def test_install_shared_bundle_zsh(tmp_path, url):
    main = tmp_path / "main"
    other = tmp_path / "other"
    main.mkdir()
    other.mkdir()
    scratch = tmp_path / "scratch" / "qq"
    scratch.parent.mkdir()
    installer, _ = make_scripts(tmp_path, [other], shared=True, main_home=main)

    result = run_script(installer, main, url, "--zsh", scratch=scratch)

    assert result.returncode == 0, result.stdout + result.stderr
    # non-interactive shells use qq directly, interactive shells the scratch copy
    bundle = str(main / ".qq" / "qq")
    assert run_shell(other, "zsh", "-c", "qq where").strip() == bundle
    assert run_shell(other, "zsh", "-ic", "qq where").strip() == str(scratch)


@needs_rsync
def test_install_shared_bundle_without_scratch(tmp_path, url):
    main = tmp_path / "main"
    main.mkdir()
    scratch = tmp_path / "scratch" / "qq"
    installer, _ = make_scripts(tmp_path, [main], shared=True, main_home=main)

    result = run_script(installer, main, url, scratch=scratch)

    assert result.returncode == 0, result.stdout + result.stderr
    assert run_shell(main, "bash", "-lc", "qq where").strip() == str(
        main / ".qq" / "qq"
    )


def test_install_shared_bundle_uses_scratch_of_user(tmp_path, url):
    main = tmp_path / "main"
    main.mkdir()
    installer, _ = make_scripts(tmp_path, [main], shared=True, main_home=main)

    result = run_script(installer, main, url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert '"/scratch/${USER}/qq/"' in (main / ".qq" / "env.bash").read_text()
    if shutil.which("fish") is not None:
        assert run_script(installer, main, url, "--fish").returncode == 0
        fish_conf = (main / ".config" / "fish" / "conf.d" / "qq.fish").read_text()
        assert '"/scratch/$USER/qq/"' in fish_conf
        assert "${USER}" not in fish_conf


def test_install_shared_bundle_missing_main_home(tmp_path, home, url):
    installer, _ = make_scripts(
        tmp_path, [home], shared=True, main_home=tmp_path / "missing"
    )

    result = run_script(installer, home, url)

    assert result.returncode == 1
    assert "main home directory" in result.stderr
    assert list(home.iterdir()) == []


@needs_zsh
def test_install_zsh(tmp_path, home, url):
    (home / ".zshrc").write_text("autoload -Uz compinit\ncompinit\n")
    # the block in .zshenv must come after the user's own PATH settings
    (home / ".zshenv").write_text("export PATH=/usr/bin:/bin\n")
    target = tmp_path / "job"
    target.mkdir()
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url, "--zsh")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "zsh (non-interactive)" in result.stdout
    zshrc = (home / ".zshrc").read_text()
    assert zshrc.startswith("autoload -Uz compinit\ncompinit\n\n")
    assert zshrc.endswith(f"{END}\n")
    zshenv = (home / ".zshenv").read_text()
    assert zshenv.startswith("export PATH=/usr/bin:/bin\n\n")
    assert zshenv.endswith(f"{END}\n")
    assert (home / ".qq" / "path.zsh").is_file()
    assert run_shell(home, "zsh", "-c", "qq --version").strip() == VERSION
    # non-interactive shells get qq, but not the shell function
    assert run_shell(home, "zsh", "-c", "whence -w qq").strip() == "qq: command"
    assert run_shell(home, "zsh", "-ic", f"qq cd {target} && pwd").strip() == str(
        target
    )


@needs_fish
def test_install_fish(tmp_path, home, url):
    target = tmp_path / "job"
    target.mkdir()
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url, "--fish")

    assert result.returncode == 0, result.stdout + result.stderr
    assert (home / ".config" / "fish" / "conf.d" / "qq.fish").is_file()
    assert run_shell(home, "fish", "-c", "qq --version").strip() == VERSION
    assert run_shell(home, "fish", "-c", f"qq cd {target}; and pwd").strip() == str(
        target
    )
    assert run_shell(home, "fish", "-c", "qq cd --help").strip() == "--help"


@pytest.mark.parametrize("args", [["--bash"], ["--zsh", "extra"], ["-x"]])
def test_install_rejects_unknown_options(tmp_path, home, url, args):
    installer, _ = make_scripts(tmp_path, [home])

    result = run_script(installer, home, url, *args)

    assert result.returncode == 1
    assert "Unknown option" in result.stderr
    assert list(home.iterdir()) == []


def test_uninstall_rejects_shell_options(tmp_path, home, url):
    _, uninstaller = make_scripts(tmp_path, [home])

    result = run_script(uninstaller, home, url, "--zsh")

    assert result.returncode == 1
    assert "Unknown option" in result.stderr


@pytest.mark.parametrize("help_option", ["-h", "--help"])
def test_scripts_print_help(tmp_path, home, url, help_option):
    installer, uninstaller = make_scripts(tmp_path, [home])

    assert "--zsh" in run_script(installer, home, url, help_option).stdout
    assert "Removes qq" in run_script(uninstaller, home, url, help_option).stdout
    assert list(home.iterdir()) == []


@pytest.mark.parametrize(
    "files",
    [
        {},
        {".bashrc": "#!/bin/bash\n\nexport A=1\n", ".profile": "export B=2\n"},
        {
            ".bashrc": "\n\nexport A=1\n\n",
            ".bash_profile": ". ~/.bashrc\n",
            ".zshrc": "x=1\n",
            ".zshenv": "\nexport Y=2\n",
        },
        {".bash_profile": "", ".bash_login": "\n"},
    ],
)
def test_uninstall_restores_original_files(tmp_path, home, url, files):
    for name, content in files.items():
        (home / name).write_text(content)
    flags = [arg for arg in ("--zsh", "--fish") if shutil.which(arg[2:])]
    installer, uninstaller = make_scripts(tmp_path, [home])
    assert run_script(installer, home, url, *flags).returncode == 0

    result = run_script(uninstaller, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    for name in (
        ".bashrc",
        ".bash_profile",
        ".bash_login",
        ".profile",
        ".zshenv",
        ".zshrc",
    ):
        if name in files:
            assert (home / name).read_text() == files[name]
        else:
            assert not (home / name).exists()
    assert not (home / ".qq").exists()
    assert not (home / ".config" / "fish" / "conf.d" / "qq.fish").exists()


def test_uninstall_removes_legacy_installation(tmp_path, home, url):
    make_bundle(home / "qq")
    (home / ".bashrc").write_text(
        "export A=1\n\n" + LEGACY_BLOCK.replace("{home}", str(home)) + "\n"
    )
    (home / ".profile").write_text(LEGACY_PROFILE)
    (home / ".bash_profile").write_text(LEGACY_PROFILE + "export B=2\n")
    _, uninstaller = make_scripts(tmp_path, [home])

    result = run_script(uninstaller, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (home / ".bashrc").read_text() == "export A=1\n"
    assert not (home / ".profile").exists()
    assert (home / ".bash_profile").read_text() == LEGACY_PROFILE + "export B=2\n"
    assert not (home / "qq").exists()


def test_uninstall_without_installation(tmp_path, home, url):
    (home / ".bashrc").write_text("export A=1\n")
    _, uninstaller = make_scripts(tmp_path, [home])

    result = run_script(uninstaller, home, url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (home / ".bashrc").read_text() == "export A=1\n"


@pytest.mark.parametrize(
    ("cluster", "action", "expected"),
    [
        ("generic", "install", "qq-install.sh"),
        ("generic", "uninstall", "qq-uninstall.sh"),
        ("metacentrum", "install", "qq-metacentrum-install.sh"),
        ("karolina", "uninstall", "qq-karolina-uninstall.sh"),
    ],
)
def test_build_script_name(cluster, action, expected):
    assert build.script_name(cluster, action) == expected


@pytest.mark.parametrize("version", ["", "v1.2.3", "1.2.3; rm -rf ~", "1.2.3\n"])
def test_build_script_rejects_invalid_version(version):
    with pytest.raises(ValueError, match="invalid qq version"):
        build.build_script(SOURCE_DIR / "clusters" / "generic.sh", "install", version)


def test_build_all(tmp_path):
    written = build.build_all(tmp_path, "0.14.0.dev2")

    names = sorted(path.name for path in written)
    clusters = sorted(path.stem for path in (SOURCE_DIR / "clusters").glob("*.sh"))
    assert len(names) == 2 * len(clusters)
    assert "qq-install.sh" in names
    assert "qq-metacentrum-uninstall.sh" in names
    for path in written:
        content = path.read_text()
        assert content.startswith("#!/usr/bin/env bash\n")
        assert 'QQ_VERSION="0.14.0.dev2"' in content
        assert os.access(path, os.X_OK)


def test_built_scripts_pass_shellcheck(tmp_path):
    shellcheck = shutil.which("shellcheck")
    assert shellcheck is not None, "shellcheck is missing, run 'uv sync --all-groups'"
    written = build.build_all(tmp_path, VERSION)

    result = subprocess.run(
        [shellcheck, "--severity=style", *map(str, written)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout


def test_install_from_terminal_does_not_hang(tmp_path, home, url):
    installer, _ = make_scripts(tmp_path, [home])
    env = {
        "HOME": str(home),
        "USER": "tester",
        "PATH": os.environ["PATH"],
        "QQ_RELEASE_BASE_URL": url,
    }
    flags = [arg for arg in ("--zsh", "--fish") if shutil.which(arg[2:])]

    exit_code = run_in_terminal(["bash", str(installer), *flags], env, timeout=60)

    assert exit_code == 0
