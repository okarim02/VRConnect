# Build & test

The target platform is **Windows** (the BLE GATT server uses the native Windows APIs through the
`windows` crate).

```bash
cargo build --release          # binary: target/release/vrconnect.exe
cargo test --lib               # unit tests (preferred runner)
cargo clippy                   # zero warnings required
cargo fmt --check              # LF line endings (.gitattributes)
```

Run it against a local configuration file:

```bash
target\release\vrconnect.exe --config-file .\.env
```

## Conventions

- SRS annotation on every function and every test:
  `/// ID SRS: SRS-FN-XXXXX` (or `SRS-MOD-` / `SRS-TEST-`) + `/// Version: V1.0`
- Tests that share global state: `#[serial]`
- Branches: `main` (stable), `dev` (integration), plus `fix/…` and `feat/…` branches
- Commit messages follow Conventional Commits (`feat(ble): …`, `fix(ble): …`)

## Chaos Monkey

`src/utils/chaos.rs` simulates packet drops, a full disk or network jitter. It is enabled **only if**
`APP_ENV` is `development`/`dev`/`staging`/`test`/`local` **and** `ENABLE_CHAOS_MONKEY=true`.

This is an **allow-list**: an empty, missing or unknown `APP_ENV` disables it. Never replace it with
a check that only rejects `production`. A warning line at startup reports any active chaos.

## Documentation

This site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/) from the
`docs/` folder and published by the `pages` job of `.gitlab-ci.yml`. To preview it locally:

```bash
pip install mkdocs-material
mkdocs serve
```
