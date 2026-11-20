## Pyinstaller run
uv run pyinstaller --name qq --onedir --clean --copy-metadata readchar src/main.py

## Installation scripts
The installers and uninstallers are generated from `scripts/installation`. `lib.sh` contains the shared logic and each file in `clusters/` configures one cluster. To build the scripts for a release, run

```
uv run python scripts/installation/build.py <version> --output dist/installation
```

where `<version>` is the release tag without the leading `v`. The release workflow does this automatically. The scripts are tested in `tests/installation`, which requires `curl`, `rsync`, and optionally `zsh` and `fish`.
